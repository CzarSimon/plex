package consumer

import (
	"net/http"

	"github.com/CzarSimon/plex/pkg/schema"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

// WebsocketHandler handles consumers connected via websockets.
type WebsocketHandler struct {
	conn *websocket.Conn
}

// NewWebsocketHandler creates a new WebsocketHandler.
func NewWebsocketHandler(w http.ResponseWriter, r *http.Request) (WebsocketHandler, error) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return WebsocketHandler{}, err
	}
	return WebsocketHandler{conn: conn}, nil
}

// Handle handles incomming messages by sending them
// over the websocket connection if still open.
func (h WebsocketHandler) Handle(msg schema.Message) error {
	err := h.conn.WriteJSON(msg)
	if err == websocket.ErrCloseSent {
		return ErrConsumerClosed
	}
	return err
}
