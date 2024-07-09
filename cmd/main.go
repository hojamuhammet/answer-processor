package main

import (
	"answers-processor/config"
	"answers-processor/internal/infrastructure/rabbitmq"
	"answers-processor/internal/infrastructure/smpp"
	"answers-processor/internal/repository"
	database "answers-processor/pkg/database"
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

	dbInstance, err := database.NewDatabase(cfg.Database.Addr)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to MySQL", "error", err)
		os.Exit(1)
	}
	defer dbInstance.Close()

	smppClient, err := smpp.NewSMPPClient(cfg, logInstance)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to initialize SMPP client", "error", err)
		os.Exit(1)
	}

	rabbitMQConn, err := rabbitmq.NewConnection(cfg.RabbitMQ.URL)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitMQConn.Close()

	rabbitmq.ConsumeMessages(rabbitMQConn, dbInstance, smppClient, logInstance)
}
