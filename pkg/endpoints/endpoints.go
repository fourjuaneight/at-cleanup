package endpoints

import (
	"log/slog"
	"sync"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/fourjuaneight/at-cleanup/pkg/store"
)

type API struct {
	Logger         *slog.Logger
	Store          *store.Store
	Directory      identity.Directory
	MagicHeaderVal string
	// inProgress tracks DIDs that currently have an enqueue operation in flight,
	// preventing concurrent POST requests from creating duplicate jobs for the same account.
	inProgress sync.Map
}

func NewAPI(
	logger *slog.Logger,
	store *store.Store,
	magicHeaderVal string,
) (*API, error) {
	dir := identity.DefaultDirectory()

	return &API{
		Logger:         logger.With("component", "api"),
		Store:          store,
		Directory:      dir,
		MagicHeaderVal: magicHeaderVal,
	}, nil
}
