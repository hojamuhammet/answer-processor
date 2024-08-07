package delivery

import (
	"answers-processor/pkg/logger"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type WebSocketServer struct {
	clients   map[*websocket.Conn]string
	broadcast chan BroadcastMessage
	upgrader  websocket.Upgrader
	mu        sync.Mutex
	Log       *logger.Loggers
}

type BroadcastMessage struct {
	Dst     string
	Message []byte
}

// NewWebSocketServer creates a new WebSocketServer instance.
func NewWebSocketServer(logInstance *logger.Loggers) *WebSocketServer {
	return &WebSocketServer{
		clients:   make(map[*websocket.Conn]string),
		broadcast: make(chan BroadcastMessage),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		Log: logInstance,
	}
}

// HandleConnections handles new WebSocket connections.
func (server *WebSocketServer) HandleConnections(w http.ResponseWriter, r *http.Request) {
	dst := r.URL.Query().Get("dst")
	if dst == "" {
		http.Error(w, "Missing dst parameter", http.StatusBadRequest)
		return
	}

	ws, err := server.upgrader.Upgrade(w, r, nil)
	if err != nil {
		server.Log.ErrorLogger.Error("Failed to upgrade connection", "error", err)
		return
	}

	server.mu.Lock()
	server.clients[ws] = dst
	server.mu.Unlock()

	server.Log.InfoLogger.Info("Client connected", "dst", dst)

	go server.readPump(ws, dst)
}

// readPump reads messages from the WebSocket connection.
func (server *WebSocketServer) readPump(conn *websocket.Conn, dst string) {
	defer func() {
		server.cleanupConnection(conn, dst)
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			server.Log.ErrorLogger.Error("Error reading message", "error", err)
			return
		}
	}
}

// cleanupConnection handles the removal and logging of a disconnected WebSocket connection.
func (server *WebSocketServer) cleanupConnection(conn *websocket.Conn, dst string) {
	server.mu.Lock()
	delete(server.clients, conn)
	server.mu.Unlock()
	conn.Close()
	server.Log.InfoLogger.Info("Client disconnected", "dst", dst)
}

// HandleMessages listens for messages on the broadcast channel and sends them to clients.
func (server *WebSocketServer) HandleMessages() {
	for broadcastMessage := range server.broadcast {
		server.mu.Lock()
		for client, dst := range server.clients {
			if dst == broadcastMessage.Dst {
				err := client.WriteMessage(websocket.TextMessage, broadcastMessage.Message)
				if err != nil {
					server.Log.ErrorLogger.Error("Failed to write message to client, closing connection", "dst", dst, "error", err)
					server.cleanupConnection(client, dst)
				}
			}
		}
		server.mu.Unlock()
	}
}

// Broadcast sends a message to the broadcast channel.
func (server *WebSocketServer) Broadcast(dst string, message []byte) {
	server.broadcast <- BroadcastMessage{Dst: dst, Message: message}
}

// Shutdown gracefully closes all WebSocket connections
func (server *WebSocketServer) Shutdown() {
	server.mu.Lock()
	defer server.mu.Unlock()

	for client := range server.clients {
		err := client.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Server shutting down"))
		if err != nil {
			server.Log.ErrorLogger.Error("Error sending close message", "error", err)
		}
		client.Close()
		delete(server.clients, client)
	}

	close(server.broadcast) // Close the broadcast channel
	server.Log.InfoLogger.Info("WebSocket server shut down gracefully")
}
