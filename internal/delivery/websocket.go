package delivery

import (
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type WebSocketServer struct {
	clients   map[*websocket.Conn]string
	broadcast chan BroadcastMessage
	upgrader  websocket.Upgrader
	mu        sync.Mutex
}

type BroadcastMessage struct {
	Dst     string
	Message []byte
}

func NewWebSocketServer() *WebSocketServer {
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
	}
}

func (server *WebSocketServer) HandleConnections(w http.ResponseWriter, r *http.Request) {
	dst := r.URL.Query().Get("dst")
	if dst == "" {
		http.Error(w, "Missing dst parameter", http.StatusBadRequest)
		return
	}

	ws, err := server.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	server.mu.Lock()
	server.clients[ws] = dst
	server.mu.Unlock()

	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			server.mu.Lock()
			delete(server.clients, ws)
			server.mu.Unlock()
			break
		}
	}
}

func (server *WebSocketServer) HandleMessages() {
	for {
		broadcastMessage := <-server.broadcast
		server.mu.Lock()
		for client, dst := range server.clients {
			if dst == broadcastMessage.Dst {
				err := client.WriteMessage(websocket.TextMessage, broadcastMessage.Message)
				if err != nil {
					client.Close()
					delete(server.clients, client)
				}
			}
		}
		server.mu.Unlock()
	}
}

func (server *WebSocketServer) Broadcast(dst string, message []byte) {
	server.broadcast <- BroadcastMessage{Dst: dst, Message: message}
}
