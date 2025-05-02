package bus

import (
	"context"
	"sync"
)

// Command is the base interface for commands.
type Command interface{}

// CommandHandler and CommandMiddleware types.
type CommandHandler = HandlerFunc[Command]
type CommandMiddleware = Middleware[Command]

// CommandBus handles command subscription and publishing.
type CommandBus[M any] struct {
	publisher  Publisher[M]
	subscriber Subscriber[M]
	handlers   sync.Map // topic -> []handler
}

// Publisher-only constructor
func NewCommandPublisher[M any](pub Publisher[M]) *CommandBus[M] {
	return &CommandBus[M]{publisher: pub}
}

// Subscriber-only constructor
func NewCommandSubscriber[M any](sub Subscriber[M]) *CommandBus[M] {
	return &CommandBus[M]{subscriber: sub}
}

// Full bus constructor
func NewCommandBus[M any](pub Publisher[M], sub Subscriber[M]) *CommandBus[M] {
	return &CommandBus[M]{publisher: pub, subscriber: sub}
}

// Publish sends a command to all subscribed handlers for the topic.
func (b *CommandBus[M]) Publish(ctx context.Context, topic string, msg M) error {
	if b.publisher == nil {
		return ErrNoPublisher
	}
	return b.publisher.Publish(ctx, topic, msg)
}

func (b *CommandBus[M]) Subscribe(topic string, handler func(ctx context.Context, msg M) error) error {
	if b.subscriber == nil {
		return ErrNoSubscriber
	}
	return b.subscriber.Subscribe(topic, handler)
}
