package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"answers-processor/config"
	websocket "answers-processor/internal/delivery"
	consumer "answers-processor/internal/infrastructure/rabbitmq/consumer"
	publisher "answers-processor/internal/infrastructure/rabbitmq/publisher"
	"answers-processor/internal/repository"
	"answers-processor/internal/service"
	db "answers-processor/pkg/database"
	"answers-processor/pkg/logger"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Setup logger
	logInstance, err := logger.SetupLogger(cfg.Env)
	if err != nil {
		log.Fatalf("Failed to set up logger: %v", err)
		os.Exit(1)
	}

	// Initialize repository (e.g., database connections)
	repository.Init(logInstance)

	// Initialize the database
	dbInstance, err := db.NewDatabase(cfg.Database.Addr)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to MySQL", "error", err)
		os.Exit(1)
	}
	defer dbInstance.Close()

	logInstance.InfoLogger.Info("Database connection successfully established.")

	// Initialize WebSocketServer
	wsServer := websocket.NewWebSocketServer(logInstance)
	logInstance.InfoLogger.Info("WebSocket server initialized.")

	// Initialize RabbitMQ Publisher Client (for sending messages)
	rabbitmqPublisher, err := publisher.NewPublisherClient(cfg, logInstance)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to create RabbitMQ publisher client", "error", err)
		os.Exit(1)
	}
	defer rabbitmqPublisher.Close()

	// Initialize the main service
	serviceInstance := service.NewService(dbInstance, rabbitmqPublisher, wsServer, logInstance)

	// Initialize RabbitMQ Consumer Client (for receiving messages)
	rabbitmqConsumer, err := consumer.NewRabbitMQClient(
		cfg.RabbitMQ.URL,
		cfg.RabbitMQ.Consumer.ExchangeName,
		cfg.RabbitMQ.Consumer.QueueName,
		cfg.RabbitMQ.Consumer.RoutingKey,
		logInstance,
		serviceInstance, // Pass the service instance to the consumer
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to initialize RabbitMQ consumer client", "error", err)
		os.Exit(1)
	}
	defer rabbitmqConsumer.Close()

	logInstance.InfoLogger.Info("RabbitMQ connections successfully established for both consumer and publisher.")

	// Start WebSocket message handler
	go wsServer.HandleMessages()

	// Start WebSocket connections handlers
	http.HandleFunc("/ws/quiz", wsServer.HandleConnections)
	http.HandleFunc("/ws/voting", wsServer.HandleConnections)
	http.HandleFunc("/ws/shop", wsServer.HandleConnections)

	// Start the WebSocket server
	go func() {
		if err := http.ListenAndServe(cfg.WebSocket.Addr, nil); err != nil {
			logInstance.ErrorLogger.Error("WebSocket server failed", "error", err)
		}
	}()

	// Handle incoming messages from RabbitMQ Consumer
	go func() {
		logInstance.InfoLogger.Info("Starting to consume messages from RabbitMQ")
		rabbitmqConsumer.ConsumeMessages(serviceInstance)
	}()

	// Handle graceful shutdown
	handleGracefulShutdown(rabbitmqConsumer, rabbitmqPublisher, wsServer, logInstance)
}

// handleGracefulShutdown listens for system signals and shuts down gracefully
func handleGracefulShutdown(rabbitmqConsumer *consumer.RabbitMQClient, rabbitmqPublisher *publisher.PublisherClient, wsServer *websocket.WebSocketServer, logInstance *logger.Loggers) {
	// Capture system signals for graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs // Block until a signal is received

	logInstance.InfoLogger.Info("Received shutdown signal, initiating graceful shutdown...")

	var wg sync.WaitGroup

	// Gracefully close RabbitMQ consumer client
	wg.Add(1)
	go func() {
		defer wg.Done()
		rabbitmqConsumer.Close()
	}()

	// Gracefully close RabbitMQ publisher client
	wg.Add(1)
	go func() {
		defer wg.Done()
		rabbitmqPublisher.Close()
	}()

	// Gracefully shut down WebSocket server
	wg.Add(1)
	go func() {
		defer wg.Done()
		wsServer.Shutdown()
	}()

	wg.Wait() // Wait for all shutdown routines to finish

	logInstance.InfoLogger.Info("Graceful shutdown complete.")
}
