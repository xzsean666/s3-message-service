package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/sean/s3-message-service/internal/application"
	"github.com/sean/s3-message-service/internal/core/cursors"
	"github.com/sean/s3-message-service/internal/storage"
)

type Server struct {
	service *application.Service
	mux     *http.ServeMux
}

func NewServer(service *application.Service) *Server {
	server := &Server{
		service: service,
		mux:     http.NewServeMux(),
	}
	server.routes()
	return server
}

func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	server.mux.ServeHTTP(response, request)
}

func (server *Server) routes() {
	server.mux.HandleFunc("GET /healthz", server.handleHealth)
	server.mux.HandleFunc("POST /v1/messages", server.handleSendMessage)
	server.mux.HandleFunc("GET /v1/messages/", server.handleGetMessage)
	server.mux.HandleFunc("GET /v1/mailboxes/", server.handleListMailbox)
	server.mux.HandleFunc("GET /v1/threads/", server.handleListThread)
	server.mux.HandleFunc("POST /v1/broadcasts", server.handleSendBroadcast)
	server.mux.HandleFunc("GET /v1/broadcasts/", server.handleGetBroadcast)
	server.mux.HandleFunc("POST /v1/states/read", server.handleMarkRead)
	server.mux.HandleFunc("POST /v1/attachments", server.handleCreateAttachment)
	server.mux.HandleFunc("GET /v1/attachments/", server.handleGetAttachment)
}

func (server *Server) handleHealth(response http.ResponseWriter, request *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (server *Server) handleSendMessage(response http.ResponseWriter, request *http.Request) {
	var command application.SendMessageCommand
	if !decodeJSON(response, request, &command) {
		return
	}
	result, err := server.service.SendMessage(request.Context(), command)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, result)
}

func (server *Server) handleGetMessage(response http.ResponseWriter, request *http.Request) {
	messageID := strings.TrimPrefix(request.URL.Path, "/v1/messages/")
	message, err := server.service.GetMessage(request.Context(), messageID)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, message)
}

func (server *Server) handleListMailbox(response http.ResponseWriter, request *http.Request) {
	parts := splitPath(strings.TrimPrefix(request.URL.Path, "/v1/mailboxes/"))
	if len(parts) != 2 {
		writeJSON(response, http.StatusNotFound, errorBody("not_found", "expected /v1/mailboxes/{actorId}/{inbox|sent}"))
		return
	}
	limit := parseLimit(request)
	result, err := server.service.ListMailbox(request.Context(), parts[0], parts[1], request.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (server *Server) handleListThread(response http.ResponseWriter, request *http.Request) {
	threadID := strings.TrimPrefix(request.URL.Path, "/v1/threads/")
	if strings.Contains(threadID, "/") {
		writeJSON(response, http.StatusNotFound, errorBody("not_found", "expected /v1/threads/{threadId}"))
		return
	}
	limit := parseLimit(request)
	result, err := server.service.ListThread(request.Context(), threadID, request.URL.Query().Get("actorId"), request.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (server *Server) handleSendBroadcast(response http.ResponseWriter, request *http.Request) {
	var command application.SendBroadcastCommand
	if !decodeJSON(response, request, &command) {
		return
	}
	result, err := server.service.SendBroadcast(request.Context(), command)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, result)
}

func (server *Server) handleGetBroadcast(response http.ResponseWriter, request *http.Request) {
	broadcastID := strings.TrimPrefix(request.URL.Path, "/v1/broadcasts/")
	broadcast, err := server.service.GetBroadcast(request.Context(), broadcastID)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, broadcast)
}

func (server *Server) handleMarkRead(response http.ResponseWriter, request *http.Request) {
	var command application.MarkReadCommand
	if !decodeJSON(response, request, &command) {
		return
	}
	result, err := server.service.MarkRead(request.Context(), command)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, result)
}

func (server *Server) handleCreateAttachment(response http.ResponseWriter, request *http.Request) {
	var command application.CreateAttachmentCommand
	if !decodeJSON(response, request, &command) {
		return
	}
	result, err := server.service.CreateAttachment(request.Context(), command)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, result)
}

func (server *Server) handleGetAttachment(response http.ResponseWriter, request *http.Request) {
	attachmentID := strings.TrimPrefix(request.URL.Path, "/v1/attachments/")
	metadata, err := server.service.GetAttachment(request.Context(), attachmentID)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, metadata)
}

func decodeJSON(response http.ResponseWriter, request *http.Request, target any) bool {
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeJSON(response, http.StatusBadRequest, errorBody("bad_request", err.Error()))
		return false
	}
	return true
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func writeError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, application.ErrValidation), errors.Is(err, cursors.ErrInvalidCursor):
		writeJSON(response, http.StatusBadRequest, errorBody("validation_error", err.Error()))
	case errors.Is(err, storage.ErrObjectNotFound):
		writeJSON(response, http.StatusNotFound, errorBody("not_found", err.Error()))
	case errors.Is(err, storage.ErrObjectAlreadyExists):
		writeJSON(response, http.StatusConflict, errorBody("already_exists", err.Error()))
	default:
		writeJSON(response, http.StatusInternalServerError, errorBody("internal_error", err.Error()))
	}
}

func errorBody(code string, message string) map[string]string {
	return map[string]string{
		"error":   code,
		"message": message,
	}
}

func parseLimit(request *http.Request) int {
	raw := request.URL.Query().Get("limit")
	if raw == "" {
		return 0
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return limit
}

func splitPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}
