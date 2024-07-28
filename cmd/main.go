package main

import (
	"answers-processor/config"
	websocket "answers-processor/internal/delivery"
	"answers-processor/internal/infrastructure/message_broker"
	"answers-processor/internal/infrastructure/rabbitmq"
	"answers-processor/internal/repository"
	"answers-processor/internal/service"
	db "answers-processor/pkg/database"
	"answers-processor/pkg/logger"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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

	messageBroker, err := message_broker.NewMessageBrokerClient(cfg, logInstance)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to create message broker client", "error", err)
		os.Exit(1)
	}

	wsServer := websocket.NewWebSocketServer(logInstance)

	go wsServer.HandleMessages()

	http.HandleFunc("/ws/quiz", wsServer.HandleConnections)
	http.HandleFunc("/ws/voting", wsServer.HandleConnections)
	http.HandleFunc("/ws/shop", wsServer.HandleConnections)

	go func() {
		if err := http.ListenAndServe(cfg.WebSocket.Addr, nil); err != nil {
			logInstance.ErrorLogger.Error("WebSocket server failed", "error", err)
		}
	}()

	rabbitMQClient, err := rabbitmq.NewRabbitMQClient(cfg.RabbitMQ.URL, logInstance)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to initialize RabbitMQ client", "error", err)
		os.Exit(1)
	}
	defer rabbitMQClient.Close()

	serviceInstance := service.NewService(dbInstance, messageBroker, wsServer, logInstance)

	go func() {
		logInstance.InfoLogger.Info("Starting to consume messages")
		rabbitMQClient.ConsumeMessages(serviceInstance)
	}()

	// Handle graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		logInstance.InfoLogger.Info("Shutting down gracefully...")
		rabbitMQClient.Close()
		os.Exit(0)
	}()

	select {}
}
