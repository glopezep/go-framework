package domain

type Entity interface {
	EntityID() string
	EntityName() string
}

type BaseEntity struct {
	entityID   string
	entityName string
}

func (e *BaseEntity) EntityID() string          { return e.entityID }
func (e *BaseEntity) EntityName() string        { return e.entityName }
func (e *BaseEntity) SetEntityID(id string)     { e.entityID = id }
func (e *BaseEntity) SetEntityName(name string) { e.entityName = name }
