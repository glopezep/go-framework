package integration

import (
	"context"

	"github.com/glopezep/framework/inbox"
)

func InboxMiddleware(store inbox.Repository) SubscribeMiddleware {
	return func(next EventHandler) EventHandler {
		return func(ctx context.Context, event Event) error {
			id := event.Metadata()["id"].(string)
			processed, err := store.Exists(ctx, id)
			if err != nil {
				return err
			}
			if processed {
				return nil // skip duplicate
			}
			return next(ctx, event)
		}
	}
}
