package rabbitmq

import (
	"answers-processor/internal/domain"
	"answers-processor/internal/service"
	"encoding/json"

	"github.com/streadway/amqp"
)

func NewConnection(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}

func ConsumeMessages(conn *amqp.Connection, service *service.Service) {
	channel, err := conn.Channel()
	if err != nil {
		service.LogInstance.ErrorLogger.Error("Failed to open a channel", "error", err)
		return
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(
		"sms.turkmentv", "direct", true, false, false, false, nil,
	)
	if err != nil {
		service.LogInstance.ErrorLogger.Error("Failed to declare an exchange", "error", err)
		return
	}

	queue, err := channel.QueueDeclare(
		"sms.turkmentv", true, false, false, false, nil,
	)
	if err != nil {
		service.LogInstance.ErrorLogger.Error("Failed to declare a queue", "error", err)
		return
	}

	err = channel.QueueBind(
		queue.Name, "", "sms.turkmentv", false, nil,
	)
	if err != nil {
		service.LogInstance.ErrorLogger.Error("Failed to bind queue to exchange", "error", err)
		return
	}

	msgs, err := channel.Consume(
		queue.Name, "", true, false, false, false, nil,
	)
	if err != nil {
		service.LogInstance.ErrorLogger.Error("Failed to register a consumer", "error", err)
		return
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var smsMessage domain.SMSMessage
			err := json.Unmarshal(d.Body, &smsMessage)
			if err != nil {
				service.LogInstance.ErrorLogger.Error("Failed to unmarshal message", "error", err)
				continue
			}

			service.LogInstance.InfoLogger.Info("Received a message", "src", smsMessage.Source, "dst", smsMessage.Destination, "txt", smsMessage.Text, "date", smsMessage.Date, "parts", smsMessage.Parts)

			service.ProcessMessage(smsMessage)
		}
	}()

	<-forever
}
