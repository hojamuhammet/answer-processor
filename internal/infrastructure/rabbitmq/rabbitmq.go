package rabbitmq

import (
	websocket "answers-processor/internal/delivery"
	"answers-processor/internal/domain"
	"answers-processor/internal/infrastructure/message_broker"
	"answers-processor/internal/service"
	"answers-processor/pkg/logger"
	"database/sql"
	"encoding/json"

	"github.com/streadway/amqp"
)

func NewConnection(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}

func ConsumeMessages(conn *amqp.Connection, db *sql.DB, messageBroker *message_broker.MessageBrokerClient, logInstance *logger.Loggers, wsServer *websocket.WebSocketServer) {
	channel, err := conn.Channel()
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to open a channel", "error", err)
		return
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(
		"sms.turkmentv", "direct", true, false, false, false, nil,
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to declare an exchange", "error", err)
		return
	}

	queue, err := channel.QueueDeclare(
		"sms.turkmentv", true, false, false, false, nil,
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to declare a queue", "error", err)
		return
	}

	err = channel.QueueBind(
		queue.Name, "", "sms.turkmentv", false, nil,
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to bind queue to exchange", "error", err)
		return
	}

	msgs, err := channel.Consume(
		queue.Name, "", true, false, false, false, nil,
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to register a consumer", "error", err)
		return
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			logInstance.InfoLogger.Info("Received a message", "message", string(d.Body))

			var smsMessage domain.SMSMessage
			err := json.Unmarshal(d.Body, &smsMessage)
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to unmarshal message", "error", err)
				continue
			}

			logInstance.InfoLogger.Info("Passing db instance to ProcessMessage")
			service.ProcessMessage(db, messageBroker, wsServer, smsMessage, logInstance)
		}
	}()

	<-forever
}
