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
	// Load the configuration
	cfg := config.LoadConfig()

	// Set up the logger
	logInstance, err := logger.SetupLogger(cfg.Env)
	if err != nil {
		log.Fatalf("Failed to set up logger: %v", err)
		os.Exit(1)
	}

	// Initialize the repository
	repository.Init(logInstance)

	// Connect to the database
	dbInstance, err := db.NewDatabase(cfg.Database.Addr)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to MySQL", "error", err)
		os.Exit(1)
	}
	defer dbInstance.Close()

	logInstance.InfoLogger.Info("Database connection successfully established.")

	// Initialize the WebSocket server
	wsServer := websocket.NewWebSocketServer(logInstance)
	logInstance.InfoLogger.Info("WebSocket server initialized.")

	// Initialize RabbitMQ publisher
	rabbitmqPublisher, err := publisher.NewRabbitmqPublisher(cfg, logInstance)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to create RabbitMQ publisher client", "error", err)
		os.Exit(1)
	}
	defer rabbitmqPublisher.Close()

	// Initialize the service with the database, publisher, and WebSocket server
	serviceInstance := service.NewService(dbInstance, rabbitmqPublisher, wsServer, logInstance)

	// Initialize RabbitMQ consumer
	rabbitmqConsumer, err := consumer.NewRabbitMQConsumer(
		cfg.RabbitMQ.URL,
		cfg.RabbitMQ.Consumer.ExchangeName,
		cfg.RabbitMQ.Consumer.QueueName,
		cfg.RabbitMQ.Consumer.RoutingKey,
		logInstance,
		serviceInstance,
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to initialize RabbitMQ consumer client", "error", err)
		os.Exit(1)
	}
	defer rabbitmqConsumer.Close()

	logInstance.InfoLogger.Info("RabbitMQ connections successfully established for both consumer and publisher.")

	go wsServer.HandleMessages()

	http.HandleFunc("/ws/quiz", wsServer.HandleConnections)
	http.HandleFunc("/ws/voting", wsServer.HandleConnections)
	http.HandleFunc("/ws/shop", wsServer.HandleConnections)

	go func() {
		logInstance.InfoLogger.Info("Starting to consume messages from RabbitMQ")
		rabbitmqConsumer.ConsumeMessages(serviceInstance)
	}()

	logInstance.InfoLogger.Info("Starting WebSocket server")
	if err := http.ListenAndServe(cfg.WebSocket.Addr, nil); err != nil {
		logInstance.ErrorLogger.Error("Failed to start server", "error", err)
		os.Exit(1)
	}

	// Handle graceful shutdown
	handleGracefulShutdown(rabbitmqConsumer, rabbitmqPublisher, wsServer, logInstance)
}

func handleGracefulShutdown(rabbitmqConsumer *consumer.RabbitMQConsumer, rabbitmqPublisher *publisher.RabbitmqPublisher, wsServer *websocket.WebSocketServer, logInstance *logger.Loggers) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	logInstance.InfoLogger.Info("Received shutdown signal, initiating graceful shutdown...")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		rabbitmqConsumer.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		rabbitmqPublisher.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		wsServer.Shutdown()
	}()

	wg.Wait()

	logInstance.InfoLogger.Info("Graceful shutdown complete.")
}
