package main

import (
	"answers-processor/config"
	"answers-processor/internal/infrastructure/message_broker"
	"answers-processor/internal/infrastructure/rabbitmq"
	"answers-processor/internal/repository"
	db "answers-processor/pkg/database"
	"answers-processor/pkg/logger"
	"log"
	"os"
)

func main() {
	cfg := config.LoadConfig()

	logInstance, err := logger.SetupLogger(cfg.Env)
	if err != nil {
		log.Fatalf("failed to set up logger: %v", err)
		os.Exit(1)
	}

	repository.Init(logInstance)

	dbInstance, err := db.NewDatabase(cfg.Database.Addr)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to MySQL", "error", err)
		os.Exit(1)
	}
	defer dbInstance.Close()

	message_broker, err := message_broker.NewMessageBrokerClient(cfg, logInstance)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to create relay client", "error", err)
		os.Exit(1)
	}

	rabbitMQConn, err := rabbitmq.NewConnection(cfg.RabbitMQ.URL)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitMQConn.Close()

	rabbitmq.ConsumeMessages(rabbitMQConn, dbInstance, message_broker, logInstance)
}
