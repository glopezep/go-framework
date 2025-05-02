package domain

// AggregateRoot is implemented by aggregates that record domain events.
type AggregateRoot interface {
	Entity
	Events() []DomainEvent
	ClearEvents()
	AddEvent(event DomainEvent)
}

// BaseAggregate provides a default implementation for event recording.
type BaseAggregate struct {
	events     []DomainEvent
	entityID   string
	entityName string
}

// AddEvent adds a domain event to the aggregate.
func (a *BaseAggregate) AddEvent(event DomainEvent) {
	meta := event.Metadata()
	meta.Set(MetaAggregateID, a.entityID)
	meta.Set(MetaAggregateName, a.entityName)
	a.events = append(a.events, event)
}

// Events returns the recorded events (does not clear them).
func (a *BaseAggregate) Events() []DomainEvent {
	return a.events
}

// ClearEvents clears all recorded events.
func (a *BaseAggregate) ClearEvents() {
	a.events = nil
}

// Entity interface methods
func (a *BaseAggregate) EntityID() string   { return a.entityID }
func (a *BaseAggregate) EntityName() string { return a.entityName }

func (a *BaseAggregate) SetEntityID(id string)     { a.entityID = id }
func (a *BaseAggregate) SetEntityName(name string) { a.entityName = name }
