package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/glopezep/framework/inbox"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultStreamName     = "INTEGRATION_EVENTS"
	defaultStreamSubjects = "events.>"
	defaultMaxAge         = 24 * time.Hour
	defaultMaxMsgs        = -1 // unlimited
)

type StreamConfig struct {
	Name     string
	Subjects []string
	MaxAge   time.Duration
	MaxMsgs  int64
}

type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

type Subscriber interface {
	Subscribe(subject string, handler func(ctx context.Context, event Event) error) error
}

type HandlerResult struct {
	HandlerID string
	Success   bool
	Error     error
}

type JetStreamBus struct {
	nc                  *nats.Conn
	js                  jetstream.JetStream
	cfg                 StreamConfig
	handlers            sync.Map
	ctx                 context.Context
	cancel              context.CancelFunc
	inboxRepo           inbox.Repository
	subscribeMiddleware []SubscribeMiddleware
	publishMiddleware   []PublishMiddleware
}

func NewJetStreamBus(url string, cfg *StreamConfig, inboxRepo inbox.Repository) (*JetStreamBus, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream context: %w", err)
	}

	if cfg == nil {
		cfg = &StreamConfig{
			Name:     defaultStreamName,
			Subjects: []string{defaultStreamSubjects},
			MaxAge:   defaultMaxAge,
			MaxMsgs:  defaultMaxMsgs,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	bus := &JetStreamBus{
		nc:        nc,
		js:        js,
		cfg:       *cfg,
		ctx:       ctx,
		cancel:    cancel,
		inboxRepo: inboxRepo,
	}

	if err := bus.ensureStream(); err != nil {
		nc.Close()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

	// Start consuming messages
	go bus.consume()

	return bus, nil
}

func (b *JetStreamBus) Publish(ctx context.Context, event Event) error {
	return b.wrapPublish(b.publishActual)(ctx, event)
}

// Subscribe registers a handler for a specific event name (subject).
func (b *JetStreamBus) Subscribe(subject string, handler EventHandler) error {
	wrapped := b.wrapSubscribe(handler)
	value, _ := b.handlers.LoadOrStore(subject, make([]EventHandler, 0))
	handlers := value.([]EventHandler)
	handlers = append(handlers, wrapped)
	b.handlers.Store(subject, handlers)
	return nil
}

func (b *JetStreamBus) ensureStream() error {
	stream, err := b.js.Stream(context.Background(), b.cfg.Name)
	if err == nil && stream != nil {
		return nil
	}

	_, err = b.js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     b.cfg.Name,
		Subjects: b.cfg.Subjects,
		MaxAge:   b.cfg.MaxAge,
		MaxMsgs:  b.cfg.MaxMsgs,
		Storage:  jetstream.FileStorage,
	})
	return err
}

func (b *JetStreamBus) consume() {
	consumerConfig := jetstream.ConsumerConfig{
		Name:          "PULL_CONSUMER",
		FilterSubject: "events.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	consumer, err := b.js.CreateOrUpdateConsumer(context.Background(), b.cfg.Name, consumerConfig)
	if err != nil {
		// Handle error, maybe log it
		return
	}

	consumption, err := consumer.Consume(func(msg jetstream.Msg) {
		var envelope map[string]interface{}
		if err := json.Unmarshal(msg.Data(), &envelope); err != nil {
			msg.Nak()
			return
		}

		messageID := envelope["id"].(string)

		// Check if message was already processed
		processed, err := b.inboxRepo.Exists(b.ctx, messageID)
		if err != nil {
			msg.Nak()
			return
		}
		if processed {
			msg.Ack()
			return
		}

		occurredAt, _ := time.Parse(time.RFC3339Nano, envelope["occurred_at"].(string))
		event := &BaseEvent{
			name:       envelope["name"].(string),
			payload:    envelope["payload"],
			metadata:   envelope["metadata"].(map[string]interface{}),
			occurredAt: occurredAt,
		}

		var handlerError error
		if handlersValue, ok := b.handlers.Load(event.Name()); ok {
			handlers := handlersValue.([]EventHandler)
			for _, handler := range handlers {
				if err := handler(b.ctx, event); err != nil {
					handlerError = err
				}
			}
		}

		if handlerError != nil {
			msg.Nak()
			return
		}

		// Save to inbox after successful processing
		inboxMsg := &inbox.Message{
			ID:            messageID,
			Name:          envelope["name"].(string),
			AggregateID:   getString(envelope, "aggregate_id"),
			AggregateName: getString(envelope, "aggregate_name"),
			Subject:       msg.Subject(),
			Data:          msg.Data(),
			Metadata:      []byte{},   // Fill as needed
			SentAt:        time.Now(), // Or parse from envelope if available
		}
		if err := b.inboxRepo.Save(b.ctx, inboxMsg); err != nil {
			msg.Nak()
			return
		}

		msg.Ack()
	})
	if err != nil {
		// Handle error, maybe log it
		return
	}

	// Wait for context cancellation
	<-b.ctx.Done()
	consumption.Stop()
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (b *JetStreamBus) Close() {
	b.cancel()
	b.nc.Close()
}

func (b *JetStreamBus) UseSubscribe(mw SubscribeMiddleware) {
	b.subscribeMiddleware = append(b.subscribeMiddleware, mw)
}

func (b *JetStreamBus) UsePublish(mw PublishMiddleware) {
	b.publishMiddleware = append(b.publishMiddleware, mw)
}

func (b *JetStreamBus) wrapSubscribe(handler EventHandler) EventHandler {
	for i := len(b.subscribeMiddleware) - 1; i >= 0; i-- {
		handler = b.subscribeMiddleware[i](handler)
	}
	return handler
}

func (b *JetStreamBus) wrapPublish(pf PublishFunc) PublishFunc {
	for i := len(b.publishMiddleware) - 1; i >= 0; i-- {
		pf = b.publishMiddleware[i](pf)
	}
	return pf
}

func (b *JetStreamBus) publishActual(ctx context.Context, event Event) error {
	envelope := map[string]interface{}{
		"id":          nats.NewInbox(),
		"name":        event.Name(),
		"payload":     event.Payload(),
		"metadata":    event.Metadata(),
		"occurred_at": event.OccurredAt().UTC().Format(time.RFC3339Nano),
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	subject := fmt.Sprintf("events.%s", event.Name())
	_, err = b.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	return nil
}
