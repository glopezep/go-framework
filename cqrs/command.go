package cqrs

import (
	"context"
	"fmt"

	"github.com/glopezep/framework/domain"
)

// Command is the base interface for all commands.
type Command interface {
	CommandType() string
	Metadata() domain.Metadata
}

// CommandHandlerFunc is a function that handles a command.
type CommandHandlerFunc func(ctx context.Context, cmd Command) error

// CommandMiddleware is a function that wraps a CommandHandlerFunc.
type CommandMiddleware func(CommandHandlerFunc) CommandHandlerFunc

// CommandHandler interface (optional, for legacy or OOP style)
type CommandHandler interface {
	Handle(ctx context.Context, cmd Command) error
}

// BaseCommand provides a default implementation for commands.
type BaseCommand struct {
	Type string
	Meta domain.Metadata
}

func (c *BaseCommand) CommandType() string { return c.Type }
func (c *BaseCommand) Metadata() domain.Metadata {
	if c.Meta == nil {
		c.Meta = make(domain.Metadata)
	}
	return c.Meta
}

// NewCommandHandler creates a CommandHandlerFunc with middleware support.
func NewCommandHandler(flusher *AggregateFlusher, handler CommandHandlerFunc, mws ...CommandMiddleware) CommandHandlerFunc {
	// Always add FlushingCommandMiddleware as the last middleware
	mws = append(mws, FlushingCommandMiddleware(flusher))
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}
	return handler
}

func LoggingCommandMiddleware(next CommandHandlerFunc) CommandHandlerFunc {
	return func(ctx context.Context, cmd Command) error {
		fmt.Printf("Handling command: %s\n", cmd.CommandType())
		return next(ctx, cmd)
	}
}

// FlushingCommandMiddleware returns a middleware that flushes events from the given aggregates after the handler runs.
func FlushingCommandMiddleware(flusher *AggregateFlusher) CommandMiddleware {
	return func(next CommandHandlerFunc) CommandHandlerFunc {
		return func(ctx context.Context, cmd Command) error {
			err := next(ctx, cmd)
			if err != nil {
				return err
			}
			aggs := AggregatesFromContext(ctx)
			return flusher.Flush(ctx, aggs...)
		}
	}
}
