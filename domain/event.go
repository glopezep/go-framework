package domain

import "github.com/google/uuid"

type BaseEvent struct {
	id      string
	name    string
	payload any
	meta    Metadata
}

func (e *BaseEvent) EventType() string { return e.name }
func (e *BaseEvent) EventID() string   { return e.id }
func (e *BaseEvent) Payload() any      { return e.payload }
func (e *BaseEvent) Metadata() Metadata {
	if e.meta == nil {
		e.meta = make(Metadata)
	}
	return e.meta
}

func NewEvent(name string, payload any) *BaseEvent {
	return &BaseEvent{
		id:      uuid.NewString(),
		name:    name,
		payload: payload,
		meta:    make(Metadata),
	}
}
