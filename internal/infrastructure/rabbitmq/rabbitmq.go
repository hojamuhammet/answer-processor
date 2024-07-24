package rabbitmq

import (
	"answers-processor/internal/domain"
	"answers-processor/internal/service"
	"encoding/json"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	reconnectDelay = 5 * time.Second
)

var (
	connMutex sync.Mutex
)

// NewConnection establishes a new connection to RabbitMQ.
func NewConnection(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}

// ConsumeMessages manages message consumption and handles reconnection logic.
func ConsumeMessages(url string, svc *service.Service) {
	var wg sync.WaitGroup
	reconnect := make(chan struct{})

	go func() {
		for range reconnect {
			svc.LogInstance.InfoLogger.Info("Attempting to reconnect to RabbitMQ...")
			for {
				conn, err := reconnectRabbitMQ(url)
				if err != nil {
					svc.LogInstance.ErrorLogger.Error("Failed to reconnect to RabbitMQ, retrying...", "error", err)
					time.Sleep(reconnectDelay)
					continue
				}
				svc.LogInstance.InfoLogger.Info("Successfully reconnected to RabbitMQ")
				startConsuming(conn, svc, &wg, reconnect)
				break
			}
		}
	}()

	conn, err := NewConnection(url)
	if err != nil {
		svc.LogInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		reconnect <- struct{}{}
	} else {
		startConsuming(conn, svc, &wg, reconnect)
	}

	wg.Wait()
}

// startConsuming sets up RabbitMQ configurations and starts consuming messages.
func startConsuming(conn *amqp.Connection, svc *service.Service, wg *sync.WaitGroup, reconnect chan struct{}) {
	channel, err := conn.Channel()
	if err != nil {
		svc.LogInstance.ErrorLogger.Error("Failed to open a channel", "error", err)
		reconnect <- struct{}{}
		return
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(
		"sms.turkmentv", "direct", true, false, false, false, nil,
	)
	if err != nil {
		svc.LogInstance.ErrorLogger.Error("Failed to declare an exchange", "error", err)
		reconnect <- struct{}{}
		return
	}

	queue, err := channel.QueueDeclare(
		"sms.turkmentv", true, false, false, false, nil,
	)
	if err != nil {
		svc.LogInstance.ErrorLogger.Error("Failed to declare a queue", "error", err)
		reconnect <- struct{}{}
		return
	}

	err = channel.QueueBind(
		queue.Name, "", "sms.turkmentv", false, nil,
	)
	if err != nil {
		svc.LogInstance.ErrorLogger.Error("Failed to bind queue to exchange", "error", err)
		reconnect <- struct{}{}
		return
	}

	msgs, err := channel.Consume(
		queue.Name, "", true, false, false, false, nil,
	)
	if err != nil {
		svc.LogInstance.ErrorLogger.Error("Failed to register a consumer", "error", err)
		reconnect <- struct{}{}
		return
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			wg.Add(1)
			go func(delivery amqp.Delivery) {
				defer wg.Done()
				processMessage(delivery, svc)
			}(d)
		}

		svc.LogInstance.ErrorLogger.Error("Channel closed, initiating reconnection...")
		reconnect <- struct{}{}
	}()

	<-forever
}

// reconnectRabbitMQ handles reconnection attempts to RabbitMQ with retries.
func reconnectRabbitMQ(url string) (*amqp.Connection, error) {
	connMutex.Lock()
	defer connMutex.Unlock()

	conn, err := NewConnection(url)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// processMessage processes each received message.
func processMessage(d amqp.Delivery, svc *service.Service) {
	var smsMessage domain.SMSMessage
	err := json.Unmarshal(d.Body, &smsMessage)
	if err != nil {
		svc.LogInstance.ErrorLogger.Error("Failed to unmarshal message", "error", err)
		return
	}

	svc.LogInstance.InfoLogger.Info("Received a message", "src", smsMessage.Source, "dst", smsMessage.Destination, "txt", smsMessage.Text, "date", smsMessage.Date, "parts", smsMessage.Parts)

	svc.ProcessMessage(smsMessage)
}
