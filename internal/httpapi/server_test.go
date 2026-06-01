package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sean/s3-message-service/internal/application"
	"github.com/sean/s3-message-service/internal/core/ids"
	"github.com/sean/s3-message-service/internal/core/keys"
	"github.com/sean/s3-message-service/internal/storage/localfs"
)

func TestHTTPServerSendAndGetMessage(t *testing.T) {
	store, err := localfs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service := application.NewService(application.Options{
		Store:               store,
		KeyBuilder:          keys.NewBuilder(""),
		IDGenerator:         ids.NewGenerator(),
		Clock:               func() time.Time { return time.Date(2026, 6, 1, 11, 22, 33, 0, time.UTC) },
		MaxPageSize:         50,
		ReadLookbackMinutes: 120,
	})
	server := NewServer(service)

	body := []byte(`{
		"senderActorId": "actor-a",
		"recipientActorIds": ["actor-b"],
		"messageType": "text",
		"payload": {"text": "hello"}
	}`)
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/messages", bytes.NewReader(body))
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("expected created, got %d: %s", response.Code, response.Body.String())
	}
	var sendResult application.SendMessageResult
	if err := json.Unmarshal(response.Body.Bytes(), &sendResult); err != nil {
		t.Fatal(err)
	}
	if sendResult.MessageID == "" {
		t.Fatalf("expected message id in response: %+v", sendResult)
	}

	getRequest := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/messages/"+sendResult.MessageID, nil)
	getResponse := httptest.NewRecorder()
	server.ServeHTTP(getResponse, getRequest)
	if getResponse.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", getResponse.Code, getResponse.Body.String())
	}
}
