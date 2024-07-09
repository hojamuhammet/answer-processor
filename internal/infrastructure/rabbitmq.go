package rabbitmq

import (
	"answers-processor/internal/service"
	"answers-processor/pkg/logger"
	"database/sql"
	"encoding/json"

	"github.com/streadway/amqp"
)

func NewConnection(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}

func ConsumeMessages(conn *amqp.Connection, db *sql.DB, logInstance *logger.Loggers) {
	channel, err := conn.Channel()
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to open a channel", "error", err)
		return
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(
		"sms.turkmentv", // name
		"direct",        // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to declare an exchange", "error", err)
		return
	}

	queue, err := channel.QueueDeclare(
		"sms.turkmentv", // name of the queue
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to declare a queue", "error", err)
		return
	}

	err = channel.QueueBind(
		queue.Name,      // queue name
		"",              // routing key
		"sms.turkmentv", // exchange
		false,
		nil,
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to bind queue to exchange", "error", err)
		return
	}

	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to register a consumer", "error", err)
		return
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			logInstance.InfoLogger.Info("Received a message", "message", string(d.Body))

			var smsMessage service.SMSMessage
			err := json.Unmarshal(d.Body, &smsMessage)
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to unmarshal message", "error", err)
				continue
			}

			service.ProcessMessage(db, smsMessage, logInstance)
		}
	}()

	<-forever
}
