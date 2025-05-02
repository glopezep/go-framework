package bus

import "errors"

var (
	ErrNoPublisher  = errors.New("bus: no publisher configured")
	ErrNoSubscriber = errors.New("bus: no subscriber configured")
)
