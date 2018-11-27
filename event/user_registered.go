package event

import (
	"encoding/json"

	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/rmb-userauth-users/model"
	"github.com/pkg/errors"
)

func userRegistered(coll *mongo.Collection, event *cmodel.Event) error {
	user := &model.User{}
	err := json.Unmarshal(event.Data, user)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling event-data to user")
		return err
	}

	_, err = coll.InsertOne(user)
	if err != nil {
		err = errors.Wrap(err, "Error inserting user into database")
		return err
	}
	return nil
}
