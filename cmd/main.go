package main

import (
	"answers-processor/config"
	rabbitmq "answers-processor/internal/infrastructure"
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

	rabbitMQConn, err := rabbitmq.NewConnection(cfg.RabbitMQ.URL)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitMQConn.Close()

	rabbitmq.ConsumeMessages(rabbitMQConn, dbInstance, logInstance)
}
