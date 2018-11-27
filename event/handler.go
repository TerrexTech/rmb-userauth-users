package event

import (
	"fmt"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

// Handle handles the provided event.
func Handle(coll *mongo.Collection, eosToken string, event *model.Event) error {
	if coll == nil {
		return errors.New("coll cannot be nil")
	}
	if event == nil || event.Action == eosToken {
		return nil
	}

	switch event.Action {
	case "UserRegistered":
		err := userRegistered(coll, event)
		if err != nil {
			err = errors.Wrap(err, "Error processing UserRegistered-event")
			return err
		}
		return nil

	case "UserDeleted":
		err := userDeleted(coll, event)
		if err != nil {
			err = errors.Wrap(err, "Error processing UserDeleted-event")
			return err
		}
		return nil

	case "UserUpdated":
		err := userUpdated(coll, event)
		if err != nil {
			err = errors.Wrap(err, "Error processing UserUpdated-event")
			return err
		}
		return nil

	default:
		return fmt.Errorf("unregistered Action: %s", event.Action)
	}
}
