package flashsale

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

func flashSaleCreated(collection *mongo.Collection, event *model.Event) *model.Document {
	flashSale := &FlashSale{}
	err := json.Unmarshal(event.Data, flashSale)
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

	if flashSale.FlashSaleID == (uuuid.UUID{}) {
		err = errors.New("missing FlashSaleID")
		err = errors.Wrap(err, "Insert")
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
	if len(flashSale.Items) == 0 {
		err = errors.New("missing FlashSaleItems")
		err = errors.Wrap(err, "Insert")
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
	if flashSale.Timestamp == 0 {
		err = errors.New("missing Timestamp")
		err = errors.Wrap(err, "Insert")
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

	marshalItems, err := json.Marshal(flashSale)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling FlashSaleItems")
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

	uuid, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error generating UUID")
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

	cid := event.CorrelationID
	if cid == (uuuid.UUID{}) {
		cid, err = uuuid.NewV4()
		err = errors.Wrap(err, "Error generating UUID")
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

	_, err = collection.FindOne(map[string]interface{}{
		"flashSaleID": flashSale.FlashSaleID.String(),
	})
	if err == nil {
		err = errors.New("the flashSale is already inserted")
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

	mm := map[string]interface{}{}
	json.Unmarshal(marshalItems, &mm)
	e := model.Event{
		AggregateID:   2,
		CorrelationID: cid,
		EventAction:   "update",
		ServiceAction: "createFlashSale",
		Data:          marshalItems,
		NanoTime:      time.Now().UnixNano(),
		UUID:          uuid,
		Version:       0,
		YearBucket:    2018,
	}

	if producer == nil {
		kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
		producer, err = kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: *commonutil.ParseHosts(kafkaBrokersStr),
		})
		if err != nil {
			err = errors.Wrap(err, "ValidateFlashSale: Error creating producer")
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
	}

	topic := os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")
	me, _ := json.Marshal(e)
	producer.Input() <- kafka.CreateMessage(topic, me)
	return nil
}
