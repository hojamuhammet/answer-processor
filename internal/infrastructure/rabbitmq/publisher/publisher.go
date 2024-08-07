package publisher

import (
	"answers-processor/config"
	"answers-processor/internal/domain"
	"answers-processor/pkg/logger"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	RetryDelay     = 2 * time.Second
	ReconnectDelay = 5 * time.Second
)

type PublisherClient struct {
	conn            *amqp.Connection
	channel         *amqp.Channel
	Logger          *logger.Loggers
	exchange        string
	queue           string
	routingKey      string
	url             string
	mu              sync.Mutex
	done            chan struct{}
	reconnecting    bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
}

// NewPublisherClient initializes a new PublisherClient.
func NewPublisherClient(cfg *config.Config, loggers *logger.Loggers) (*PublisherClient, error) {
	client := &PublisherClient{
		Logger:     loggers,
		exchange:   cfg.RabbitMQ.Publisher.ExchangeName,
		queue:      cfg.RabbitMQ.Publisher.QueueName,
		routingKey: cfg.RabbitMQ.Publisher.RoutingKey,
		url:        cfg.RabbitMQ.URL,
		done:       make(chan struct{}),
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	go client.monitorConnection()

	return client, nil
}

// Connection

func (c *PublisherClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanupConnection()

	conn, err := amqp.Dial(c.url)
	if err != nil {
		c.Logger.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		c.Logger.ErrorLogger.Error("Failed to open a channel", "error", err)
		return err
	}

	c.conn = conn
	c.channel = channel

	// Re-establish notification channels for monitoring
	c.notifyConnClose = c.conn.NotifyClose(make(chan *amqp.Error))
	c.notifyChanClose = c.channel.NotifyClose(make(chan *amqp.Error))

	if err := c.setupChannel(); err != nil {
		c.cleanupConnection()
		c.Logger.ErrorLogger.Error("Failed to setup RabbitMQ channel", "error", err)
		return err
	}

	c.Logger.InfoLogger.Info("RabbitMQ publisher connection and channel successfully established.")
	return nil
}

func (c *PublisherClient) setupChannel() error {
	if err := c.channel.ExchangeDeclare(
		c.exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if _, err := c.channel.QueueDeclare(
		c.queue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if err := c.channel.QueueBind(
		c.queue,
		c.routingKey,
		c.exchange,
		false,
		nil,
	); err != nil {
		return err
	}

	c.Logger.InfoLogger.Info("RabbitMQ publisher queue, exchange, and channel setup completed successfully.")
	return nil
}

// Logic

func (c *PublisherClient) SendMessage(src, dest, text string) error {
	message := domain.RelayMessage{
		Src: src,
		Dst: dest,
		Msg: text,
	}

	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	for {
		c.mu.Lock()
		err = c.channel.Publish(
			c.exchange,
			c.routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         body,
				DeliveryMode: amqp.Persistent,
			},
		)
		c.mu.Unlock()

		if err != nil {
			c.Logger.ErrorLogger.Error("Failed to publish message", "error", err)
			time.Sleep(RetryDelay)
			continue
		}

		return nil
	}
}

// Reconnection

func (c *PublisherClient) monitorConnection() {
	for {
		select {
		case err := <-c.notifyConnClose:
			if err != nil {
				c.Logger.ErrorLogger.Error("RabbitMQ publisher connection closed", "error", err)
				c.reconnect()
			}
		case err := <-c.notifyChanClose:
			if err != nil {
				c.Logger.ErrorLogger.Error("RabbitMQ publisher channel closed", "error", err)
				c.reconnect()
			}
		case <-c.done:
			return
		}
	}
}

func (c *PublisherClient) reconnect() {
	c.mu.Lock()
	if c.reconnecting {
		c.mu.Unlock()
		return
	}
	c.reconnecting = true
	c.mu.Unlock()

	go func() {
		defer func() {
			c.mu.Lock()
			c.reconnecting = false
			c.mu.Unlock()
		}()

		for {
			select {
			case <-c.done:
				return
			default:
				c.Logger.InfoLogger.Info("Attempting to reconnect RabbitMQ publisher...")

				// Cleanup and reset state
				c.cleanupConnection()

				if err := c.connect(); err == nil {
					c.Logger.InfoLogger.Info("Successfully reconnected RabbitMQ publisher.")
					go c.monitorConnection() // Restart monitoring after reconnect
					return
				}

				c.Logger.ErrorLogger.Error("RabbitMQ publisher reconnection attempt failed, retrying...")
				time.Sleep(ReconnectDelay)
			}
		}
	}()
}

func (c *PublisherClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
		// The channel is already closed, do nothing
	default:
		close(c.done)
	}
	c.cleanupConnection()
	c.Logger.InfoLogger.Info("RabbitMQ publisher connection and channel closed")
}

func (c *PublisherClient) cleanupConnection() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
