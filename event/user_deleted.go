package event

import (
	"encoding/json"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

func userDeleted(coll *mongo.Collection, event *model.Event) error {
	params := map[string]interface{}{}
	err := json.Unmarshal(event.Data, &params)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling event-data to params")
		return err
	}

	_, err = coll.DeleteMany(params)
	if err != nil {
		err = errors.Wrap(err, "Error deleting users from database")
		return err
	}
	return nil
}
