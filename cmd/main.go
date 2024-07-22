package main

import (
	"answers-processor/config"
	websocket "answers-processor/internal/delivery"
	"answers-processor/internal/infrastructure/message_broker"
	"answers-processor/internal/infrastructure/rabbitmq"
	"answers-processor/internal/repository"
	db "answers-processor/pkg/database"
	"answers-processor/pkg/logger"
	"log"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"
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

	wsServer := websocket.NewWebSocketServer()
	go wsServer.HandleMessages()

	http.HandleFunc("/ws/quiz-ws", wsServer.HandleConnections)
	go func() {
		if err := http.ListenAndServe(cfg.WebSocket.Addr, nil); err != nil {
			logInstance.ErrorLogger.Error("WebSocket server failed", "error", err)
		}
	}()

	rabbitMQConn, err := rabbitmq.NewConnection(cfg.RabbitMQ.URL)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitMQConn.Close()

	logInstance.InfoLogger.Info("Starting to consume messages")
	rabbitmq.ConsumeMessages(rabbitMQConn, dbInstance, message_broker, logInstance, wsServer)
}
