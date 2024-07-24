package message_broker

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
	MaxRetries     = 3
	RetryDelay     = 2 * time.Second
	ReconnectDelay = 5 * time.Second
)

type MessageBrokerClient struct {
	conn     *amqp.Connection
	Channel  *amqp.Channel
	Logger   *logger.Loggers
	Queue    string
	mu       sync.Mutex
	url      string
	isClosed bool
}

// NewMessageBrokerClient initializes a new MessageBrokerClient and connects to RabbitMQ.
func NewMessageBrokerClient(cfg *config.Config, loggers *logger.Loggers) (*MessageBrokerClient, error) {
	client := &MessageBrokerClient{
		Logger: loggers,
		Queue:  "sms_reply",
		url:    cfg.RabbitMQ.URL,
	}
	err := client.connect()
	if err != nil {
		return nil, err
	}
	return client, nil
}

// connect establishes a connection to RabbitMQ and opens a channel.
func (c *MessageBrokerClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := amqp.Dial(c.url)
	if err != nil {
		c.Logger.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		c.Logger.ErrorLogger.Error("Failed to open a channel", "error", err)
		return fmt.Errorf("failed to open a channel: %w", err)
	}
	_, err = ch.QueueDeclare(
		c.Queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		c.Logger.ErrorLogger.Error("Failed to declare a queue", "error", err)
		return fmt.Errorf("failed to declare a queue: %w", err)
	}

	c.conn = conn
	c.Channel = ch
	c.isClosed = false

	go c.handleReconnection()

	return nil
}

// handleReconnection listens for connection closure and attempts to reconnect.
func (c *MessageBrokerClient) handleReconnection() {
	reconnect := make(chan *amqp.Error)
	c.conn.NotifyClose(reconnect)

	go func() {
		for err := range reconnect {
			if err != nil && !c.isClosed {
				c.Logger.ErrorLogger.Error("RabbitMQ connection closed", "error", err)
				for {
					time.Sleep(ReconnectDelay)
					c.mu.Lock()
					if c.isClosed {
						c.mu.Unlock()
						return
					}
					err := c.connect()
					if err != nil {
						c.Logger.ErrorLogger.Error("Failed to reconnect to RabbitMQ", "error", err)
						c.mu.Unlock()
						continue
					}
					c.Logger.InfoLogger.Info("Successfully reconnected to RabbitMQ")
					c.mu.Unlock()
					break
				}
			}
		}
	}()
}

// Close safely closes the connection and channel.
func (c *MessageBrokerClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isClosed = true
	if err := c.Channel.Close(); err != nil {
		c.Logger.ErrorLogger.Error("Failed to close channel", "error", err)
	}
	if err := c.conn.Close(); err != nil {
		c.Logger.ErrorLogger.Error("Failed to close connection", "error", err)
	}
}

// SendMessage sends a message to RabbitMQ with retries.
func (c *MessageBrokerClient) SendMessage(src, dest, text string) error {
	c.Logger.InfoLogger.Info("Sending message via RabbitMQ", "src", src, "dst", dest, "text", text)

	message := domain.RelayMessage{
		Src: src,
		Dst: dest,
		Msg: text,
	}

	body, err := json.Marshal(message)
	if err != nil {
		c.Logger.ErrorLogger.Error("Failed to marshal message", "error", err)
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	for attempt := 0; attempt < MaxRetries; attempt++ {
		c.mu.Lock()
		err = c.Channel.Publish(
			"",      // exchange
			c.Queue, // routing key
			false,   // mandatory
			false,   // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)
		c.mu.Unlock()
		if err != nil {
			c.Logger.ErrorLogger.Error("Failed to send message via RabbitMQ", "attempt", attempt+1, "error", err)
			time.Sleep(RetryDelay)
			continue
		}

		c.Logger.InfoLogger.Info("Message sent successfully via RabbitMQ", "src", src, "dst", dest, "text", text)
		return nil
	}

	c.Logger.ErrorLogger.Error("Failed to send message via RabbitMQ after max retries", "src", src, "dst", dest, "text", text)
	return fmt.Errorf("failed to send message via RabbitMQ after %d attempts", MaxRetries)
}
