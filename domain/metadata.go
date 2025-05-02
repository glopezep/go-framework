package domain

type Metadata map[string]any

const (
	MetaAggregateID   = "aggregate_id"
	MetaAggregateName = "aggregate_name"
)

func (m Metadata) Set(key string, value any) {
	m[key] = value
}

func (m Metadata) Get(key string) (any, bool) {
	val, ok := m[key]
	return val, ok
}

func (m Metadata) Del(key string) {
	delete(m, key)
}
