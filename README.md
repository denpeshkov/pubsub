# pubsub

[![CI](https://github.com/denpeshkov/pubsub/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/denpeshkov/pubsub/actions/workflows/ci.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/denpeshkov/pubsub.svg)](https://pkg.go.dev/github.com/denpeshkov/pubsub)
[![Go Report Card](https://goreportcard.com/badge/github.com/denpeshkov/pubsub)](https://goreportcard.com/report/github.com/denpeshkov/pubsub)

An idiomatic, lightweight Go library for in-memory Publish-Subscribe messaging.

# Usage

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/denpeshkov/pubsub"
	"golang.org/x/sync/errgroup"
)

func main() {
	// Create the PubSub.
	ps := pubsub.New[string]()

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	// Start the PubSub.
	g.Go(func() error {
		ps.Run(ctx)
		return nil
	})

	// Subscribe to topics.
	sub, err := ps.Subscribe(100, "topic1", "topic2")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = ps.Unsubscribe(sub) }()

	// Start a goroutine to receive the messages.
	g.Go(func() error {
		for msg := range sub.Messages() {
			log.Println("Received message:", msg)
		}
		return nil
	})

	// Publish to topics.
	for i := range 3 {
		if err := ps.Publish(context.Background(), fmt.Sprintf("foo_%d", i), "topic1"); err != nil {
			log.Fatalf(`Failed to publish to "topic1": %v`, err)
		}
		if err := ps.Publish(context.Background(), fmt.Sprintf("bar_%d", i), "topic1"); err != nil {
			log.Fatalf(`Failed to publish to "topic2": %v`, err)
		}
	}

	// Stop the PubSub.
	cancel()
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
}
```