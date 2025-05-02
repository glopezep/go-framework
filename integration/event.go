package integration

import (
	"time"
)

// Event represents an integration event that can be published across services
type Event interface {
	// Name returns the event type name
	Name() string
	// Payload returns the event data
	Payload() interface{}
	// Metadata returns the event metadata
	Metadata() map[string]interface{}
	// OccurredAt returns when the event was created
	OccurredAt() time.Time
}

// BaseEvent provides a default implementation of Event
type BaseEvent struct {
	name       string
	payload    interface{}
	metadata   map[string]interface{}
	occurredAt time.Time
}

// NewEvent creates a new integration event
func NewEvent(name string, payload interface{}) *BaseEvent {
	return &BaseEvent{
		name:       name,
		payload:    payload,
		metadata:   make(map[string]interface{}),
		occurredAt: time.Now(),
	}
}

func (e *BaseEvent) Name() string                     { return e.name }
func (e *BaseEvent) Payload() interface{}             { return e.payload }
func (e *BaseEvent) Metadata() map[string]interface{} { return e.metadata }
func (e *BaseEvent) OccurredAt() time.Time            { return e.occurredAt }

// SetMetadata sets a metadata key-value pair
func (e *BaseEvent) SetMetadata(key string, value interface{}) {
	e.metadata[key] = value
}
