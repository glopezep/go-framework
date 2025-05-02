package integration

import (
	"context"
)

type EventHandler func(ctx context.Context, event Event) error

// For subscription (handler) middleware
type SubscribeMiddleware func(next EventHandler) EventHandler

// For publishing middleware
type PublishFunc func(ctx context.Context, event Event) error
type PublishMiddleware func(next PublishFunc) PublishFunc
