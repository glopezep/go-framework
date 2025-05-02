package integration

import (
	"context"
	"errors"
)

var (
	ErrNoPublisher  = errors.New("integration: no publisher configured")
	ErrNoSubscriber = errors.New("integration: no subscriber configured")
)

type IntegrationPublisher interface {
	Publish(ctx context.Context, event Event) error
}

type IntegrationSubscriber interface {
	Subscribe(subject string, handler func(ctx context.Context, event Event) error) error
}

type IntegrationBus struct {
	publisher  IntegrationPublisher
	subscriber IntegrationSubscriber
}

func NewIntegrationPublisher(pub IntegrationPublisher) *IntegrationBus {
	return &IntegrationBus{publisher: pub}
}

func NewIntegrationSubscriber(sub IntegrationSubscriber) *IntegrationBus {
	return &IntegrationBus{subscriber: sub}
}

func NewIntegrationBus(pub IntegrationPublisher, sub IntegrationSubscriber) *IntegrationBus {
	return &IntegrationBus{publisher: pub, subscriber: sub}
}

func (b *IntegrationBus) Publish(ctx context.Context, event Event) error {
	if b.publisher == nil {
		return ErrNoPublisher
	}
	return b.publisher.Publish(ctx, event)
}

func (b *IntegrationBus) Subscribe(subject string, handler func(ctx context.Context, event Event) error) error {
	if b.subscriber == nil {
		return ErrNoSubscriber
	}
	return b.subscriber.Subscribe(subject, handler)
}
