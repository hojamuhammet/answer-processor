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

const reconnectDelay = 5 * time.Second

type RabbitMQConsumer struct {
	conn            *amqp.Connection
	channel         *amqp.Channel
	url             string
	exchange        string
	queue           string
	routingKey      string
	logInstance     *logger.Loggers
	mu              sync.Mutex
	isShuttingDown  bool
	handler         func(amqp.Delivery)
	reconnecting    bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	done            chan struct{}
}

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

	// Re-establish notification channels for monitoring
	c.resetNotifyChannels()

	if !c.isShuttingDown {
		c.logInstance.InfoLogger.Info("RabbitMQ consumer connection and channel successfully established.")
	}
	return nil
}

func (c *RabbitMQConsumer) resetNotifyChannels() {
	c.notifyConnClose = make(chan *amqp.Error, 1)
	c.notifyChanClose = make(chan *amqp.Error, 1)
	c.conn.NotifyClose(c.notifyConnClose)
	c.channel.NotifyClose(c.notifyChanClose)
}

func (c *RabbitMQConsumer) ConsumeMessages(service *service.Service) {
	c.handler = func(msg amqp.Delivery) {
		var smsMessage domain.SMSMessage
		if err := json.Unmarshal(msg.Body, &smsMessage); err != nil {
			c.logInstance.ErrorLogger.Error("Failed to unmarshal message", "error", err)
			return
		}

		service.ProcessMessage(smsMessage)
	}
	c.consumeMessages(c.handler)
}

func (c *RabbitMQConsumer) consumeMessages(handler func(amqp.Delivery)) {
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
		case <-c.done:
			return
		}
	}
}

func (c *RabbitMQConsumer) monitorConnection() {
	for {
		select {
		case err := <-c.notifyConnClose:
			if err != nil {
				c.logInstance.ErrorLogger.Error("RabbitMQ consumer connection closed", "error", err)
				go c.reconnect()
			}
		case err := <-c.notifyChanClose:
			if err != nil {
				c.logInstance.ErrorLogger.Error("RabbitMQ consumer channel closed", "error", err)
				go c.reconnect()
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
			c.logInstance.InfoLogger.Info("Attempting to reconnect RabbitMQ consumer...")

			// Cleanup and reset state
			c.cleanupConnection()

			if err := c.connect(); err == nil {
				c.logInstance.InfoLogger.Info("Successfully reconnected RabbitMQ consumer.")
				go c.consumeMessages(c.handler) // Ensure consuming resumes after reconnection
				go c.monitorConnection()        // Restart monitoring after reconnect
				return
			}

			c.logInstance.ErrorLogger.Error("RabbitMQ consumer reconnection attempt failed, retrying...")
			time.Sleep(reconnectDelay)
		}
	}()
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
	c.logInstance.InfoLogger.Info("RabbitMQ consumer connection and channel closed")
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
