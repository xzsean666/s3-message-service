package main

import (
	"log"
	"net/http"

	"github.com/sean/s3-message-service/internal/application"
	"github.com/sean/s3-message-service/internal/config"
	"github.com/sean/s3-message-service/internal/core/ids"
	"github.com/sean/s3-message-service/internal/core/keys"
	"github.com/sean/s3-message-service/internal/httpapi"
	"github.com/sean/s3-message-service/internal/storage/localfs"
)

func main() {
	runtimeConfig, err := config.LoadFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	store, err := localfs.New(runtimeConfig.FilesystemRoot)
	if err != nil {
		log.Fatal(err)
	}

	service := application.NewService(application.Options{
		Store:               store,
		KeyBuilder:          keys.NewBuilder(runtimeConfig.ObjectNamespace),
		IDGenerator:         ids.NewGenerator(),
		MaxPageSize:         runtimeConfig.MaxPageSize,
		ReadLookbackMinutes: runtimeConfig.ReadLookbackMinutes,
	})

	server := httpapi.NewServer(service)
	log.Printf("s3-message-service listening on %s", runtimeConfig.HTTPAddress)
	if err := http.ListenAndServe(runtimeConfig.HTTPAddress, server); err != nil {
		log.Fatal(err)
	}
}
