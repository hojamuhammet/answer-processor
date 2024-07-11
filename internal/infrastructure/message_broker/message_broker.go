package message_broker

import (
	"answers-processor/config"
	"answers-processor/pkg/logger"
	"encoding/json"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

const (
	MaxRetries = 3
	RetryDelay = 2 * time.Second
)

type MessageBrokerClient struct {
	Channel *amqp.Channel
	Logger  *logger.Loggers
	Queue   string
}

type RelayMessage struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
	Msg string `json:"msg"`
}

func NewMessageBrokerClient(cfg *config.Config, loggers *logger.Loggers) (*MessageBrokerClient, error) {
	conn, err := amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		"sms_reply",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	return &MessageBrokerClient{
		Channel: ch,
		Logger:  loggers,
		Queue:   "sms_reply",
	}, nil
}

func (c *MessageBrokerClient) SendMessage(src, dest, text string) error {
	c.Logger.InfoLogger.Info("Sending message via RabbitMQ", "src", src, "dst", dest, "text", text)

	message := RelayMessage{
		Src: src,
		Dst: dest,
		Msg: text,
	}

	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	for attempt := 0; attempt < MaxRetries; attempt++ {
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
		if err != nil {
			c.Logger.ErrorLogger.Error("Failed to send message via RabbitMQ", "attempt", attempt+1, "error", err)
			time.Sleep(RetryDelay)
			continue
		}

		c.Logger.InfoLogger.Info("Message sent successfully via RabbitMQ", "src", src, "dst", dest, "text", text)
		return nil
	}

	return fmt.Errorf("failed to send message via RabbitMQ after %d attempts", MaxRetries)
}
