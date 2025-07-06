// Package pubsub implements a publish/subscribe messaging system.
package pubsub

import (
	"context"
	"errors"
	"slices"
)

var (
	// ErrPubSubClosed is returned when calling methods on a closed [PubSub].
	ErrPubSubClosed = errors.New("PubSub closed")
	// ErrMessageDropped is returned by [Publish] when one or more subscribers
	// could not receive the message because their subscription channel was full.
	ErrMessageDropped = errors.New("message dropped: subscriber channel is full")
)

// Subscription is a [PubSub] subscription.
// Subscription is created by the call to [PubSub.Subscribe].
type Subscription[T any] struct {
	closed bool
	ch     chan T
}

// Messages returns a channel to receive messages from [PubSub].
// Closing the [PubSub] will close the channel.
func (s *Subscription[T]) Messages() <-chan T { return s.ch }

// PubSub delivers messages to subscribers.
// It must be started by calling [PubSub.Run] before use.
type PubSub[T any] struct {
	subs     map[string][]*Subscription[T]
	actionCh chan func()
	closedCh chan struct{}
}

// New returns a new [PubSub] instance.
func New[T any]() *PubSub[T] {
	return &PubSub[T]{
		subs:     make(map[string][]*Subscription[T]),
		actionCh: make(chan func()),
		closedCh: make(chan struct{}),
	}
}

// Run starts the [PubSub] message processing loop.
// It blocks until the provided context is canceled, at which point
// it closes the [PubSub] and all subscriptions.
// This method must be called before any other methods on the [PubSub].
func (ps *PubSub[T]) Run(ctx context.Context) {
	defer close(ps.closedCh)
	for {
		select {
		case f := <-ps.actionCh:
			f()
		case <-ctx.Done():
			for _, subs := range ps.subs {
				for _, sub := range subs {
					if sub.closed {
						continue
					}
					sub.closed = true
					close(sub.ch)
				}
			}
			clear(ps.subs)
			return
		}
	}
}

// Subscribe creates and returns a new subscription with the specified buffer size.
// If no topics are provided, it subscribes to all currently registered topics.
func (ps *PubSub[T]) Subscribe(bufSize int, topics ...string) (*Subscription[T], error) {
	sub := &Subscription[T]{ch: make(chan T, bufSize), closed: false}
	if err := ps.process(func() {
		if len(topics) == 0 {
			for t := range ps.subs {
				ps.subs[t] = append(ps.subs[t], sub)
			}
		} else {
			for _, t := range topics {
				ps.subs[t] = append(ps.subs[t], sub)
			}
		}
	}); err != nil {
		return nil, err
	}
	return sub, nil
}

// Unsubscribe unsubscribes from the given topics.
// If no topics are provided, it unsubscribes from all currently registered topics.
func (ps *PubSub[T]) Unsubscribe(sub *Subscription[T], topics ...string) error {
	return ps.process(func() {
		if len(topics) == 0 {
			for t := range ps.subs {
				ps.unsubscribe(sub, t)
			}
		} else {
			for _, t := range topics {
				ps.unsubscribe(sub, t)
			}
		}
	})
}

func (ps *PubSub[T]) unsubscribe(sub *Subscription[T], topic string) {
	subs := ps.subs[topic]
	i := slices.Index(ps.subs[topic], sub)
	if i == -1 {
		return
	}
	// Optimization by deleting without preserving order.
	subs[i] = subs[len(subs)-1]
	subs[len(subs)-1] = nil
	subs = subs[:len(subs)-1]

	if len(subs) == 0 {
		delete(ps.subs, topic)
	} else {
		ps.subs[topic] = subs
	}
}

// Publish publishes the message to the given topic.
func (ps *PubSub[T]) Publish(ctx context.Context, msg T, topic string) error {
	chErr := make(chan error)
	if err := ps.process(func() {
		var err error
	loop:
		for _, sub := range ps.subs[topic] {
			select {
			case sub.ch <- msg:
			case <-ctx.Done():
				break loop
			default:
				err = ErrMessageDropped
			}
		}
		chErr <- err
	}); err != nil {
		return err
	}
	return <-chErr
}

// Topics returns all currently registered topics — those with at least one subscriber.
func (ps *PubSub[T]) Topics() ([]string, error) {
	ch := make(chan []string)
	if err := ps.process(func() {
		topics := make([]string, 0, len(ps.subs))
		for t := range ps.subs {
			topics = append(topics, t)
		}
		ch <- topics
	}); err != nil {
		return nil, err
	}
	t := <-ch
	return t, nil
}

func (ps *PubSub[T]) process(f func()) error {
	select {
	case ps.actionCh <- f:
		return nil
	case <-ps.closedCh:
		return ErrPubSubClosed
	}
}
