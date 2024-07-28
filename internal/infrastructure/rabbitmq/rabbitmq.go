package rabbitmq

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
	conn        *amqp.Connection
	channel     *amqp.Channel
	url         string
	logInstance *logger.Loggers
	mu          sync.Mutex
	closed      chan *amqp.Error
}

func NewRabbitMQClient(url string, logInstance *logger.Loggers) (*RabbitMQClient, error) {
	client := &RabbitMQClient{
		url:         url,
		logInstance: logInstance,
		closed:      make(chan *amqp.Error),
	}
	err := client.connect()
	if err != nil {
		return nil, err
	}
	go client.handleReconnection()
	return client, nil
}

func (c *RabbitMQClient) connect() error {
	var err error
	c.conn, err = amqp.Dial(c.url)
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		return err
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to open a channel", "error", err)
		c.conn.Close()
		return err
	}

	err = c.setupChannel()
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to set up channel", "error", err)
		c.channel.Close()
		c.conn.Close()
		return err
	}

	c.closed = c.channel.NotifyClose(make(chan *amqp.Error))

	return nil
}

func (c *RabbitMQClient) setupChannel() error {
	err := c.channel.ExchangeDeclare(
		"sms.turkmentv", "direct", true, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	_, err = c.channel.QueueDeclare(
		"sms.turkmentv", true, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	err = c.channel.QueueBind(
		"sms.turkmentv", "", "sms.turkmentv", false, nil,
	)
	if err != nil {
		return err
	}

	return nil
}

func (c *RabbitMQClient) handleReconnection() {
	for err := range c.closed {
		if err != nil {
			c.logInstance.ErrorLogger.Error("RabbitMQ connection closed", "error", err)
			for {
				time.Sleep(reconnectDelay)
				c.mu.Lock()
				err := c.connect()
				c.mu.Unlock()
				if err == nil {
					c.logInstance.InfoLogger.Info("Successfully reconnected to RabbitMQ")
					break
				}
				c.logInstance.ErrorLogger.Error("Failed to reconnect to RabbitMQ", "error", err)
			}
		}
	}
}

func (c *RabbitMQClient) ConsumeMessages(service *service.Service) {
	for {
		c.mu.Lock()
		msgs, err := c.channel.Consume(
			"sms.turkmentv", "", true, false, false, false, nil,
		)
		c.mu.Unlock()

		if err != nil {
			c.logInstance.ErrorLogger.Error("Failed to register a consumer", "error", err)
			time.Sleep(reconnectDelay)
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

		c.logInstance.ErrorLogger.Error("Message channel closed, attempting to reconnect")
	}
}

func (c *RabbitMQClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
