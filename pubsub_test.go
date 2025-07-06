package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/denpeshkov/pubsub"

	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m) // goleak requires VerifyTestMain for parallel tests.
}

func TestRun(t *testing.T) {
	t.Parallel()

	ps := pubsub.New[struct{}]()
	ctx, cancel := context.WithCancel(t.Context())
	stopCh := make(chan struct{})
	go func() {
		defer close(stopCh)
		ps.Run(ctx)
	}()

	mustSubscribe(t, ps, 1, "topic1")
	mustSubscribe(t, ps, 1, "topic1")
	mustSubscribe(t, ps, 1, "topic2")
	mustSubscribe(t, ps, 1, "topic1", "topic2", "topic3")

	// Stop the PubSub
	cancel()
	select {
	case <-stopCh:
	case <-time.After(1 * time.Second):
		t.Errorf("PubSub.Run() haven't returned after ctx cancellation")
	}

	t.Run("ErrPubSubClosed after close", func(t *testing.T) {
		t.Parallel()

		ps := pubsub.New[struct{}]()
		ctx, stop := context.WithCancel(t.Context())
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error { ps.Run(ctx); return nil })
		stop()
		if err := g.Wait(); err != nil {
			t.Errorf("Run() failed: %v", err)
		}

		for range 3 {
			sub, err := ps.Subscribe(1, "topic")
			if !errors.Is(err, pubsub.ErrPubSubClosed) {
				t.Errorf("Subscribe() error %v, want %v", err, pubsub.ErrPubSubClosed)
			}
			if err := ps.Unsubscribe(sub); !errors.Is(err, pubsub.ErrPubSubClosed) {
				t.Errorf("Unsubscribe() error %v, want %v", err, pubsub.ErrPubSubClosed)
			}
			if err := ps.Publish(t.Context(), struct{}{}, "topic"); !errors.Is(err, pubsub.ErrPubSubClosed) {
				t.Errorf("Publish() error %v, want %v", err, pubsub.ErrPubSubClosed)
			}
			if _, err := ps.Topics(); !errors.Is(err, pubsub.ErrPubSubClosed) {
				t.Errorf("Topics() error %v, want %v", err, pubsub.ErrPubSubClosed)
			}
		}
	})
}

func TestSubscribe_Publish(t *testing.T) {
	t.Parallel()

	t.Run("no subscribers", func(t *testing.T) {
		t.Parallel()

		ps := newPubSub[struct{}](t)

		mustPublish(t, ps, struct{}{}, "topic1")
		mustPublish(t, ps, struct{}{}, "topic1")
		mustPublish(t, ps, struct{}{}, "topic2")
		mustPublish(t, ps, struct{}{}, "topic2")
	})

	genSlice := func(n int) []int {
		var s []int
		for i := range n {
			s = append(s, i)
		}
		return s
	}
	for bufSize := 1; bufSize <= 10; bufSize++ {
		t.Run(fmt.Sprintf("buffer size %d", bufSize), func(t *testing.T) {
			t.Parallel()

			ps := newPubSub[int](t)

			subs := []*pubsub.Subscription[int]{
				mustSubscribe(t, ps, bufSize, "topic1"),
				mustSubscribe(t, ps, bufSize, "topic2"),
				mustSubscribe(t, ps, bufSize, "topic2"),
				mustSubscribe(t, ps, bufSize, "topic3"),
				mustSubscribe(t, ps, bufSize, "topic3"),
				mustSubscribe(t, ps, bufSize, "topic3"),
			}

			// Published message should be received.
			for v := range bufSize {
				mustPublish(t, ps, v, "topic1", "topic2", "topic3")
			}
			for _, sub := range subs {
				mustReceive(t, sub, genSlice(bufSize)...)
			}

			// Publish should not block on full channel.
			for v := range bufSize {
				mustPublish(t, ps, v, "topic1", "topic2", "topic3")
			}
			mustDropPublish(t, ps, -1, "topic1", "topic2", "topic3")
			for _, sub := range subs {
				mustReceive(t, sub, genSlice(bufSize)...)
			}
		})
	}
}

func TestSubscribe_Unsubscribe_Topics(t *testing.T) {
	t.Parallel()

	t.Run("subscribe to same topic", func(t *testing.T) {
		t.Parallel()

		ps := newPubSub[string](t)

		var subs []*pubsub.Subscription[string]
		for range 5 {
			subs = append(subs, mustSubscribe(t, ps, 1, "topic1"))
			mustPublish(t, ps, "foo", "topic1")
			for _, sub := range subs {
				mustReceive(t, sub, "foo")
			}
			mustEqualTopics(t, ps, "topic1")
		}
		for range 5 {
			subs = append(subs, mustSubscribe(t, ps, 1, "topic2"))
			mustPublish(t, ps, "foo", "topic2")
			mustNotReceive(t, subs[:5]...)
			// Only topic2 subscruptions.
			for _, sub := range subs[5:] {
				mustReceive(t, sub, "foo")
			}
			mustEqualTopics(t, ps, "topic1", "topic2")
		}
		for range 5 {
			mustPublish(t, ps, "foo", "topic1", "topic2")
			for _, sub := range subs {
				mustReceive(t, sub, "foo")
			}
			mustEqualTopics(t, ps, "topic1", "topic2")
		}
	})

	t.Run("subscribe-unsubscribe", func(t *testing.T) {
		t.Parallel()

		ps := newPubSub[string](t)

		sub := mustSubscribe(t, ps, 1, "topic")
		mustPublish(t, ps, "foo", "topic")
		mustReceive(t, sub, "foo")
		mustEqualTopics(t, ps, "topic")

		mustUnsubscribe(t, ps, sub, "topic")
		mustNotReceive(t, sub)
		mustEqualTopics(t, ps)

		sub = mustSubscribe(t, ps, 1, "topic")
		mustPublish(t, ps, "foo", "topic")
		mustReceive(t, sub, "foo")
		mustEqualTopics(t, ps, "topic")
		mustUnsubscribe(t, ps, sub, "topic")
		mustNotReceive(t, sub)
		mustEqualTopics(t, ps)
	})

	t.Run("unsubscribe unknown topic", func(t *testing.T) {
		t.Parallel()

		ps := newPubSub[string](t)

		sub := mustSubscribe(t, ps, 1, "topic")
		mustPublish(t, ps, "foo", "topic")
		mustReceive(t, sub, "foo")
		mustEqualTopics(t, ps, "topic")

		mustUnsubscribe(t, ps, sub, "topic_unknown")
		mustNotReceive(t, sub)
		mustEqualTopics(t, ps, "topic")
	})

	t.Run("subscribe one topic at a time", func(t *testing.T) {
		t.Parallel()

		ps := newPubSub[string](t)

		var (
			topics []string
			subs   []*pubsub.Subscription[string]
		)
		for i := range 5 {
			mustEqualTopics(t, ps, topics...)

			tp := fmt.Sprintf("topic%d", i+1)
			topics = append(topics, tp)

			subs = append(subs, mustSubscribe(t, ps, 1, tp))
			mustPublish(t, ps, "foo", topics...)
			for _, sub := range subs {
				mustReceive(t, sub, "foo")
			}
		}
	})

	t.Run("subscribe multiple topics at a time", func(t *testing.T) {
		t.Parallel()

		ps := newPubSub[string](t)

		var topics []string
		for i := range 5 {
			topics = append(topics, fmt.Sprintf("topic%d", i+1))
			{
				sub := mustSubscribe(t, ps, len(topics), topics...)
				mustPublish(t, ps, "foo", topics...)
				mustReceive(t, sub, slices.Repeat([]string{"foo"}, len(topics))...) // Receive from all the topics
				mustEqualTopics(t, ps, topics...)
				// Unsubscribe from specified topics.
				mustUnsubscribe(t, ps, sub, topics...)
				mustNotReceive(t, sub)
				mustEqualTopics(t, ps)
			}
			{
				sub := mustSubscribe(t, ps, len(topics), topics...)
				mustPublish(t, ps, "foo", topics...)
				mustReceive(t, sub, slices.Repeat([]string{"foo"}, len(topics))...) // Receive from all the topics
				mustEqualTopics(t, ps, topics...)
				// Unsubscribe from all active topics.
				mustUnsubscribe(t, ps, sub)
				mustNotReceive(t, sub)
				mustEqualTopics(t, ps)
			}
		}
	})

	t.Run("subscribe all active", func(t *testing.T) {
		t.Parallel()

		ps := newPubSub[string](t)

		var topics []string
		for i := range 5 {
			tp := fmt.Sprintf("topic%d", i+1)
			topics = append(topics, tp)

			sub1 := mustSubscribe(t, ps, len(topics), topics...)
			sub2 := mustSubscribe(t, ps, len(topics)) // Should be subscribed to the same topics as sub1.
			mustPublish(t, ps, "foo", topics...)
			mustReceive(t, sub1, slices.Repeat([]string{"foo"}, len(topics))...)
			mustReceive(t, sub2, slices.Repeat([]string{"foo"}, len(topics))...)
			mustEqualTopics(t, ps, topics...)
			mustUnsubscribe(t, ps, sub1, topics...)
			mustUnsubscribe(t, ps, sub2, topics...)
		}
	})

	t.Run("return on context cancel", func(t *testing.T) {
		t.Parallel()

		// Create a cancelled context.
		cancelledCtx, cancel := context.WithCancel(t.Context())
		cancel()

		ps := newPubSub[struct{}](t)
		mustSubscribe(t, ps, 0, "topic")
		if err := ps.Publish(cancelledCtx, struct{}{}, "topic"); err != nil {
			t.Errorf("Publish() failed: %v", err)
		}
	})
}

func newPubSub[T any](t *testing.T) *pubsub.PubSub[T] {
	t.Helper()

	ps := pubsub.New[T]()

	g, ctx := errgroup.WithContext(t.Context())
	g.Go(func() error { ps.Run(ctx); return nil })
	t.Cleanup(func() {
		if err := g.Wait(); err != nil {
			t.Errorf("Run() failed: %v", err)
		}
	})

	return ps
}

func mustSubscribe[T any](t *testing.T, ps *pubsub.PubSub[T], bufSize int, topics ...string) *pubsub.Subscription[T] {
	t.Helper()

	sub, err := ps.Subscribe(bufSize, topics...)
	if err != nil {
		t.Errorf("Subscribe(%d, %v) failed: %v", bufSize, topics, err)
	}
	return sub
}

func mustUnsubscribe[T any](t *testing.T, ps *pubsub.PubSub[T], sub *pubsub.Subscription[T], topics ...string) {
	t.Helper()

	if err := ps.Unsubscribe(sub, topics...); err != nil {
		t.Errorf("Unsubscribe(%v) failed: %v", topics, err)
	}
}

func mustPublish[T any](t *testing.T, ps *pubsub.PubSub[T], msg T, topics ...string) {
	t.Helper()

	chErr := make(chan error)
	publish := func(tp string) { go func() { chErr <- ps.Publish(t.Context(), msg, tp) }() }

	for _, tp := range topics {
		publish(tp)
		select {
		case err := <-chErr:
			if err != nil {
				t.Fatalf("Publish(%q) failed: %v", tp, err)
			}
		case <-time.After(10 * time.Millisecond):
			t.Errorf("Publish(%q) blocked", tp)
		}
	}
}

func mustDropPublish[T any](t *testing.T, ps *pubsub.PubSub[T], msg T, topics ...string) {
	t.Helper()

	chErr := make(chan error)
	publish := func(tp string) { go func() { chErr <- ps.Publish(t.Context(), msg, tp) }() }

	for _, tp := range topics {
		publish(tp)

		if err := <-chErr; err == nil {
			t.Errorf("Publish(%q) not blocked", tp)
		} else if !errors.Is(err, pubsub.ErrMessageDropped) {
			t.Errorf("Publish(%q) unexpected error: %v", tp, err)
		}
	}
}

func mustReceive[T comparable](t *testing.T, sub *pubsub.Subscription[T], msgs ...T) {
	t.Helper()

	for _, msg := range msgs {
		select {
		case v := <-sub.Messages():
			if v != msg {
				t.Errorf("Messages() = %v, want %v", v, msg)
			}
		case <-time.After(10 * time.Millisecond):
			t.Errorf("Messages() blocked")
		}
	}
}

func mustNotReceive[T any](t *testing.T, subs ...*pubsub.Subscription[T]) {
	t.Helper()

	for _, sub := range subs {
		select {
		case v := <-sub.Messages():
			t.Errorf("Messages() unexpected value: %v", v)
		default:
		}
	}
}

func mustEqualTopics[T any](t *testing.T, ps *pubsub.PubSub[T], want ...string) {
	t.Helper()

	got, err := ps.Topics()
	if err != nil {
		t.Errorf("Topics() failed: %v", err)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("Topics() = %v, want %v", got, want)
	}
}
