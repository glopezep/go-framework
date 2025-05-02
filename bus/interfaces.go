package bus

import (
	"context"
)

type Publisher[M any] interface {
	Publish(ctx context.Context, topic string, msg M) error
}

type Subscriber[M any] interface {
	Subscribe(topic string, handler func(ctx context.Context, msg M) error) error
}
