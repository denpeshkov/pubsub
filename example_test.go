package pubsub_test

import (
	"context"
	"fmt"

	"github.com/denpeshkov/pubsub"

	"golang.org/x/sync/errgroup"
)

func Example() {
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
		panic(err)
	}
	defer func() { _ = ps.Unsubscribe(sub) }()

	// Start a goroutine to receive the messages.
	g.Go(func() error {
		for msg := range sub.Messages() {
			fmt.Println("Received message:", msg)
		}
		return nil
	})

	// Publish to topics.
	for i := range 3 {
		if err := ps.Publish(context.Background(), fmt.Sprintf("foo_%d", i), "topic1"); err != nil {
			panic(err)
		}
		if err := ps.Publish(context.Background(), fmt.Sprintf("bar_%d", i), "topic1"); err != nil {
			panic(err)
		}
	}

	// Stop the PubSub.
	cancel()
	if err := g.Wait(); err != nil {
		panic(err)
	}

	// Output:
	// Received message: foo_0
	// Received message: bar_0
	// Received message: foo_1
	// Received message: bar_1
	// Received message: foo_2
	// Received message: bar_2
}
