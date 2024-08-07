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
	conn       *amqp.Connection
	channel    *amqp.Channel
	Logger     *logger.Loggers
	exchange   string
	queue      string
	routingKey string
	url        string
	mu         sync.Mutex
}

func NewPublisherClient(cfg *config.Config, loggers *logger.Loggers) (*PublisherClient, error) {
	client := &PublisherClient{
		Logger:     loggers,
		exchange:   cfg.RabbitMQ.Publisher.ExchangeName,
		queue:      cfg.RabbitMQ.Publisher.QueueName,
		routingKey: cfg.RabbitMQ.Publisher.RoutingKey,
		url:        cfg.RabbitMQ.URL,
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	go client.monitorConnection()

	return client, nil
}

func (c *PublisherClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil && !c.conn.IsClosed() {
		c.conn.Close()
	}

	var err error
	c.conn, err = amqp.Dial(c.url)
	if err != nil {
		c.Logger.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		return err
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		c.cleanupConnection()
		c.Logger.ErrorLogger.Error("Failed to open a channel", "error", err)
		return err
	}

	if err := c.setupChannel(); err != nil {
		c.cleanupConnection()
		c.Logger.ErrorLogger.Error("Failed to setup RabbitMQ channel", "error", err)
		return err
	}

	c.Logger.InfoLogger.Info("RabbitMQ connection and channel successfully established.")
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

	return nil
}

func (c *PublisherClient) monitorConnection() {
	for {
		select {
		case err := <-c.conn.NotifyClose(make(chan *amqp.Error)):
			if err != nil {
				c.Logger.ErrorLogger.Error("RabbitMQ connection closed", "error", err)
				c.reconnect()
			}
		case err := <-c.channel.NotifyClose(make(chan *amqp.Error)):
			if err != nil {
				c.Logger.ErrorLogger.Error("RabbitMQ channel closed", "error", err)
				c.reconnect()
			}
		}
	}
}

func (c *PublisherClient) reconnect() {
	for {
		c.cleanupConnection()

		if err := c.connect(); err == nil {
			c.Logger.InfoLogger.Info("Successfully reconnected to RabbitMQ.")
			return
		}

		c.Logger.ErrorLogger.Error("Reconnection attempt failed, retrying...")
		time.Sleep(ReconnectDelay)
	}
}

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

func (c *PublisherClient) cleanupConnection() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *PublisherClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupConnection()
	c.Logger.InfoLogger.Info("RabbitMQ connection and channel closed")
}
