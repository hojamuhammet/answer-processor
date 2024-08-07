package rabbitmq_consumer

import (
	"answers-processor/internal/domain"
	"answers-processor/internal/service"
	"answers-processor/pkg/logger"
	"encoding/json"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	reconnectDelay = 5 * time.Second
)

type RabbitMQClient struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	url            string
	exchange       string
	queue          string
	routingKey     string
	logInstance    *logger.Loggers
	mu             sync.Mutex
	isShuttingDown bool
	service        *service.Service
}

func NewRabbitMQClient(url, exchange, queue, routingKey string, logInstance *logger.Loggers, service *service.Service) (*RabbitMQClient, error) {
	client := &RabbitMQClient{
		url:         url,
		exchange:    exchange,
		queue:       queue,
		routingKey:  routingKey,
		logInstance: logInstance,
		service:     service,
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	go client.monitorConnection()

	return client, nil
}

func (c *RabbitMQClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil && !c.conn.IsClosed() {
		c.conn.Close()
	}

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

func (c *RabbitMQClient) monitorConnection() {
	for {
		select {
		case err := <-c.conn.NotifyClose(make(chan *amqp.Error)):
			if err != nil && !c.isShuttingDown {
				c.logInstance.ErrorLogger.Error("RabbitMQ connection closed", "error", err)
				c.reconnect()
			}
		case err := <-c.channel.NotifyClose(make(chan *amqp.Error)):
			if err != nil && !c.isShuttingDown {
				c.logInstance.ErrorLogger.Error("RabbitMQ channel closed", "error", err)
				c.reconnect()
			}
		}
	}
}

func (c *RabbitMQClient) reconnect() {
	for {
		c.cleanupConnection()

		if err := c.connect(); err == nil {
			c.logInstance.InfoLogger.Info("Successfully reconnected to RabbitMQ.")
			go c.ConsumeMessages(c.service)
			return
		}

		c.logInstance.ErrorLogger.Error("Reconnection attempt failed, retrying...")
		time.Sleep(reconnectDelay)
	}
}

func (c *RabbitMQClient) ConsumeMessages(service *service.Service) {
	for {
		if c.isShuttingDown {
			return
		}
		c.mu.Lock()
		msgs, err := c.channel.Consume(
			c.queue,
			"",
			true,
			false,
			false,
			false,
			nil,
		)
		c.mu.Unlock()

		if err != nil {
			if !c.isShuttingDown {
				c.logInstance.ErrorLogger.Error("Failed to register a consumer", "error", err)
			}
			c.reconnect()
			continue
		}

		for d := range msgs {
			var smsMessage domain.SMSMessage
			err := json.Unmarshal(d.Body, &smsMessage)
			if err != nil {
				c.logInstance.ErrorLogger.Error("Failed to unmarshal message", "error", err)
				continue
			}

			c.logInstance.InfoLogger.Info("Received a message", "src", smsMessage.Source, "dst", smsMessage.Destination, "txt", smsMessage.Text, "date", smsMessage.Date, "parts", smsMessage.Parts)
			service.ProcessMessage(smsMessage)
		}

		if !c.isShuttingDown {
			c.logInstance.ErrorLogger.Error("Message channel closed, attempting to reconnect")
		}
		c.reconnect()
	}
}

func (c *RabbitMQClient) cleanupConnection() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *RabbitMQClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isShuttingDown = true
	c.cleanupConnection()
	c.logInstance.InfoLogger.Info("RabbitMQ connection and channel closed")
}
