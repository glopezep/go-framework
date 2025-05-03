package framework

import (
	"context"
	"errors"

	"github.com/glopezep/framework/bus"
	"github.com/glopezep/framework/cqrs"
	"github.com/glopezep/framework/domain"
	"github.com/glopezep/framework/inbox"
	"github.com/glopezep/framework/integration"
	"github.com/glopezep/framework/outbox"
)

type Application struct {
	Flusher          *cqrs.AggregateFlusher
	InboxRepo        inbox.Repository
	OutboxRepo       outbox.Repository
	DomainDispatcher *domain.Dispatcher
	Bus              *integration.JetStreamBus // Optional
	// Add other shared dependencies as needed
	// ...other optional components
	commandHandlers map[string]cqrs.CommandHandler
	queryHandlers   map[string]cqrs.QueryHandler
}

type ApplicationOption func(*Application)

// NewFramework initializes and wires up all framework components.
func NewApplication(
	inboxRepo inbox.Repository,
	outboxRepo outbox.Repository,
	opts ...ApplicationOption,
) *Application {
	eventBus := bus.NewEventBus()
	domainDispatcher := domain.NewDispatcher(eventBus)
	flusher := cqrs.NewAggregateFlusher(domainDispatcher)

	app := &Application{
		Flusher:          flusher,
		InboxRepo:        inboxRepo,
		OutboxRepo:       outboxRepo,
		DomainDispatcher: domainDispatcher,
		commandHandlers:  make(map[string]cqrs.CommandHandler),
		queryHandlers:    make(map[string]cqrs.QueryHandler),
	}

	for _, opt := range opts {
		opt(app)
	}

	return app
}

func WithJetStreamBus(bus *integration.JetStreamBus) ApplicationOption {
	return func(app *Application) {
		app.Bus = bus
	}
}

// RegisterCommandHandler registers a command handler by name.
func (a *Application) RegisterCommandHandler(name string, handler cqrs.CommandHandler) {
	a.commandHandlers[name] = handler
}

// RegisterQueryHandler registers a query handler by name.
func (a *Application) RegisterQueryHandler(name string, handler cqrs.QueryHandler) {
	a.queryHandlers[name] = handler
}

// ExecuteCommand executes a registered command handler by name.
func (a *Application) ExecuteCommand(ctx context.Context, name string, cmd cqrs.Command) error {
	handler, ok := a.commandHandlers[name]
	if !ok {
		return errors.New("command handler not found: " + name)
	}
	return handler.Handle(ctx, cmd)
}

// ExecuteQuery executes a registered query handler by name and returns the result.
func (a *Application) ExecuteQuery(ctx context.Context, name string, qry cqrs.Query) (any, error) {
	handler, ok := a.queryHandlers[name]
	if !ok {
		return nil, errors.New("query handler not found: " + name)
	}
	return handler.Handle(ctx, qry)
}

type commandHandlerWrapper struct {
	fn cqrs.CommandHandlerFunc
}

func (w *commandHandlerWrapper) Handle(ctx context.Context, cmd cqrs.Command) error {
	return w.fn(ctx, cmd)
}

// NewCommandHandler creates a command handler with default middleware.
func (a *Application) NewCommandHandler(fn cqrs.CommandHandlerFunc, middlewares ...cqrs.CommandMiddleware) cqrs.CommandHandler {
	all := append(middlewares, cqrs.FlushingCommandMiddleware(a.Flusher))
	handlerFunc := cqrs.NewCommandHandler(a.Flusher, fn, all...)
	return &commandHandlerWrapper{fn: handlerFunc}
}

type queryHandlerWrapper struct {
	fn cqrs.QueryHandlerFunc
}

func (w *queryHandlerWrapper) Handle(ctx context.Context, qry cqrs.Query) (any, error) {
	return w.fn(ctx, qry)
}

func (a *Application) NewQueryHandler(fn cqrs.QueryHandlerFunc, middlewares ...cqrs.QueryMiddleware) cqrs.QueryHandler {
	handlerFunc := cqrs.NewQueryHandler(fn, middlewares...)
	return &queryHandlerWrapper{fn: handlerFunc}
}
