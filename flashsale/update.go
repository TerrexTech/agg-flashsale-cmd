package flashsale

import (
	"encoding/json"
	"log"

	"github.com/TerrexTech/go-commonutils/commonutil"

	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

type flashSaleUpdate struct {
	Filter map[string]interface{} `json:"filter"`
	Update map[string]interface{} `json:"update"`
}

type updateResult struct {
	MatchedCount  int64 `json:"matchedCount,omitempty"`
	ModifiedCount int64 `json:"modifiedCount,omitempty"`
}

// Update handles "update" events.
func Update(collection *mongo.Collection, event *model.Event) *model.Document {
	flashSaleUpdate := &flashSaleUpdate{}

	err := json.Unmarshal(event.Data, flashSaleUpdate)
	if err != nil {
		err = errors.Wrap(err, "Update: Error while unmarshalling Event-data")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	update := flashSaleUpdate.Update

	if len(flashSaleUpdate.Filter) == 0 {
		err = errors.New("blank filter provided")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}
	if len(update) == 0 {
		err = errors.New("blank update provided")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	if update["flashSaleID"] != nil && update["flashSaleID"] == (uuuid.UUID{}).String() {
		err = errors.New("missing flashSaleID")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	// Get Timestamp if present, assert it to Int64, and check if its 0.
	if update["timestamp"] != nil {
		timestamp, err := commonutil.AssertInt64(update["timestamp"])
		if err != nil {
			err = errors.New("error asserting Timestamp")
			err = errors.Wrap(err, "Update")
			log.Println(err)
		}
		if err == nil && timestamp == int64(0) {
			err = errors.New("missing Timestamp")
			err = errors.Wrap(err, "Update")
			log.Println(err)
		}
		if err != nil {
			return &model.Document{
				AggregateID:   event.AggregateID,
				CorrelationID: event.CorrelationID,
				Error:         err.Error(),
				ErrorCode:     InternalError,
				EventAction:   event.EventAction,
				ServiceAction: event.ServiceAction,
				UUID:          event.UUID,
			}
		}
	}

	updateStats, err := collection.UpdateMany(flashSaleUpdate.Filter, update)
	if err != nil {
		err = errors.Wrap(err, "Update: Error in UpdateMany")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     DatabaseError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	result := &updateResult{
		MatchedCount:  updateStats.MatchedCount,
		ModifiedCount: updateStats.ModifiedCount,
	}
	resultMarshal, err := json.Marshal(result)
	if err != nil {
		err = errors.Wrap(err, "Update: Error marshalling FlashSale Update-result")
		log.Println(err)
		return &model.Document{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	return &model.Document{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		EventAction:   event.EventAction,
		Result:        resultMarshal,
		ServiceAction: event.ServiceAction,
		UUID:          event.UUID,
	}
}
