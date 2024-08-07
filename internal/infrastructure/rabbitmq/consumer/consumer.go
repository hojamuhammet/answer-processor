package consumer

import (
	"encoding/json"
	"sync"
	"time"

	"answers-processor/internal/domain"
	"answers-processor/internal/service"
	"answers-processor/pkg/logger"

	"github.com/streadway/amqp"
)

const (
	reconnectDelay = 5 * time.Second
)

// RabbitMQConsumer handles consuming messages from RabbitMQ.
type RabbitMQConsumer struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	url            string
	exchange       string
	queue          string
	routingKey     string
	logInstance    *logger.Loggers
	mu             sync.Mutex
	isShuttingDown bool
	done           chan struct{}
	handler        func(amqp.Delivery)
	reconnecting   bool
}

// NewRabbitMQConsumer initializes a new RabbitMQConsumer.
func NewRabbitMQConsumer(url, exchange, queue, routingKey string, logInstance *logger.Loggers, service *service.Service) (*RabbitMQConsumer, error) {
	client := &RabbitMQConsumer{
		url:         url,
		exchange:    exchange,
		queue:       queue,
		routingKey:  routingKey,
		logInstance: logInstance,
		done:        make(chan struct{}),
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	go client.monitorConnection()

	return client, nil
}

// Connection

func (c *RabbitMQConsumer) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanupConnection()

	var err error
	c.conn, err = amqp.Dial(c.url)
	if err != nil {
		if !c.isShuttingDown {
			c.logInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		}
		return err
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		c.cleanupConnection()
		if !c.isShuttingDown {
			c.logInstance.ErrorLogger.Error("Failed to open a channel", "error", err)
		}
		return err
	}

	if !c.isShuttingDown {
		c.logInstance.InfoLogger.Info("RabbitMQ connection and channel successfully established.")
	}
	return nil
}

// Logic

func (c *RabbitMQConsumer) ConsumeMessages(service *service.Service) {
	c.handler = func(msg amqp.Delivery) {
		var smsMessage domain.SMSMessage
		if err := json.Unmarshal(msg.Body, &smsMessage); err != nil {
			c.logInstance.ErrorLogger.Error("Failed to unmarshal message", "error", err)
			return
		}

		service.ProcessMessage(smsMessage)
	}
	c.consumeMessages(c.handler, c.done)
}

func (c *RabbitMQConsumer) consumeMessages(handler func(amqp.Delivery), done <-chan struct{}) {
	msgs, err := c.channel.Consume(
		c.queue,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to start consuming messages", "error", err)
		c.reconnect()
		return
	}

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				c.logInstance.ErrorLogger.Error("Message channel closed, attempting to reconnect")
				c.reconnect()
				return
			}
			handler(msg)
		case <-done:
			return
		}
	}
}

// Reconnection

func (c *RabbitMQConsumer) monitorConnection() {
	for {
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()

		if conn == nil {
			c.logInstance.ErrorLogger.Error("Connection is nil, waiting to retry monitorConnection...")
			time.Sleep(reconnectDelay)
			continue
		}

		select {
		case err := <-conn.NotifyClose(make(chan *amqp.Error)):
			if err != nil {
				c.logInstance.ErrorLogger.Error("RabbitMQ connection closed", "error", err)
				c.reconnect()
			}
		case <-c.done:
			return
		}
	}
}

func (c *RabbitMQConsumer) reconnect() {
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
			if c.isShuttingDown {
				return
			}
			c.logInstance.InfoLogger.Info("Attempting to reconnect to RabbitMQ...")
			if err := c.connect(); err == nil {
				c.logInstance.InfoLogger.Info("Successfully reconnected to RabbitMQ.")
				c.consumeMessages(c.handler, c.done)
				return
			}

			c.logInstance.ErrorLogger.Error("Reconnection attempt failed, retrying...")
			time.Sleep(reconnectDelay)
		}
	}()
}

func (c *RabbitMQConsumer) NotifyClose() <-chan *amqp.Error {
	if c.conn == nil {
		return nil
	}
	return c.conn.NotifyClose(make(chan *amqp.Error))
}

func (c *RabbitMQConsumer) IsConnected() bool {
	return c.conn != nil && !c.conn.IsClosed()
}

func (c *RabbitMQConsumer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isShuttingDown = true

	select {
	case <-c.done:
	default:
		close(c.done)
	}

	c.cleanupConnection()
	c.logInstance.InfoLogger.Info("RabbitMQ connection and channel closed")
}

func (c *RabbitMQConsumer) cleanupConnection() {
	if c.channel != nil {
		c.channel.Close()
		c.channel = nil
	}
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
