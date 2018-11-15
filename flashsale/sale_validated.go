package flashsale

import (
	"encoding/json"
	"log"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

type flashSaleItemResult struct {
	ItemID          uuuid.UUID `json:"itemID,omitempty"`
	Error           string     `json:"error,omitempty"`
	ErrorCode       int        `json:"errorCode,omitempty"`
	TotalSoldWeight float64    `json:"totalSoldWeight,omitempty"`
	TotalWeight     float64    `json:"totalWeight,omitempty"`
}

type flashSaleValidationResp struct {
	OriginalRequest FlashSale             `json:"originalRequest,omitempty"`
	Result          []flashSaleItemResult `json:"result,omitempty"`
}

func flashSaleValidated(
	collection *mongo.Collection,
	event *model.Event,
) *model.Document {
	validResp := &flashSaleValidationResp{}
	err := json.Unmarshal(event.Data, validResp)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error while unmarshalling Event-data")
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

	_, err = collection.InsertOne(validResp.OriginalRequest)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error Inserting FlashSale into Database")
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

	result, err := json.Marshal(validResp.Result)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error marshalling FlashSale Insert-result")
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
		Result:        result,
		ServiceAction: event.ServiceAction,
		UUID:          event.UUID,
	}
}
