package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/TerrexTech/agg-inventory-cmd/inventory"
	"github.com/TerrexTech/go-mongoutils/mongo"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/agg-flashsale-cmd/flashsale"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/uuuid"
	"github.com/joho/godotenv"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func Byf(s string, args ...interface{}) {
	By(fmt.Sprintf(s, args...))
}

func TestFlashSale(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_EVENT_GROUP",

		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",

		"KAFKA_PRODUCER_EVENT_TOPIC",
		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",
		"KAFKA_PRODUCER_RESPONSE_TOPIC",

		"MONGO_HOSTS",
		"MONGO_USERNAME",
		"MONGO_PASSWORD",
		"MONGO_DATABASE",
		"MONGO_CONNECTION_TIMEOUT_MS",
		"MONGO_RESOURCE_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "FlashSaleAggregate Suite")
}

var _ = Describe("FlashSaleAggregate", func() {
	var (
		kafkaBrokers          []string
		producer              *kafka.Producer
		eventsTopic           string
		producerResponseTopic string

		mockFlashSale  *flashSale.FlashSale
		mockEvent *model.Event

		invColl *mongo.Collection
	)
	BeforeSuite(func() {
		kafkaBrokers = *commonutil.ParseHosts(
			os.Getenv("KAFKA_BROKERS"),
		)
		eventsTopic = os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")
		producerResponseTopic = os.Getenv("KAFKA_PRODUCER_RESPONSE_TOPIC")

		var err error
		producer, err = kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		})
		Expect(err).ToNot(HaveOccurred())

		flashSaleID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		itemID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())

		db := os.Getenv("MONGO_DATABASE")
		invColl, err = loadMongoCollection(db, "agg_inventory", &inventory.Inventory{})
		Expect(err).ToNot(HaveOccurred())
		mockInv := inventory.Inventory{
			ItemID:      itemID,
			Lot:         "test-lot",
			Name:        "test-name",
			Origin:      "test-origin",
			SKU:         "test-sku",
			UPC:         "test-upc",
			SoldWeight:  0,
			TotalWeight: 200,
		}
		_, err = invColl.InsertOne(mockInv)
		Expect(err).ToNot(HaveOccurred())

		mockFlashSale = &flashSale.FlashSale{
			FlashSaleID: flashSaleID,
			Items: []flashSale.SoldItem{
				flashSale.SoldItem{
					ItemID: itemID,
					UPC:    "test-upc",
					Weight: 12.24,
					Lot:    "test-lot",
					SKU:    "test-sku",
				},
			},
			Timestamp: time.Now().Unix(),
		}
		marshalFlashSale, err := json.Marshal(mockFlashSale)
		Expect(err).ToNot(HaveOccurred())

		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		uid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		mockEvent = &model.Event{
			EventAction:   "insert",
			CorrelationID: cid,
			AggregateID:   flashSale.AggregateID,
			Data:          marshalFlashSale,
			NanoTime:      time.Now().UnixNano(),
			UserUUID:      uid,
			UUID:          uuid,
			Version:       0,
			YearBucket:    2018,
		}
	})

	Describe("FlashSale Operations", func() {
		It("should insert record", func(done Done) {
			Byf("Producing MockEvent")
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			producer.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "aggflashSale.test.group.1",
				Topics:       []string{eventsTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				event := &model.Event{}
				err := json.Unmarshal(msg.Value, event)
				Expect(err).ToNot(HaveOccurred())

				if event.UUID == mockEvent.UUID {
					flashSale := &flashSale.FlashSale{}
					err = json.Unmarshal(event.Data, flashSale)

					if err == nil && flashSale.FlashSaleID == mockFlashSale.FlashSaleID {
						Expect(flashSale).To(Equal(mockFlashSale))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())

			// Wait for events to be processed and flashSale to be added.
			time.Sleep(5 * time.Second)
			// Can't search directly in Mongo if using embedded-arrays
			mockFindFlashSale := *mockFlashSale
			mockFindFlashSale.Items = []flashSale.SoldItem{}

			findResult, err := aggColl.FindOne(mockFindFlashSale)
			Expect(err).ToNot(HaveOccurred())

			findFlashSale, assertOK := findResult.(*flashSale.FlashSale)
			Expect(assertOK).To(BeTrue())
			mockFlashSale.ID = findFlashSale.ID
			Expect(findFlashSale).To(Equal(mockFlashSale))

			invColl.FindOne(map[string]interface{}{
				"itemID": mockFlashSale.Items[0].ItemID.String(),
			})
			close(done)
		}, 25)

		It("should update record", func(done Done) {
			Byf("Creating update args")
			filterFlashSale := map[string]interface{}{
				"flashSaleID": mockFlashSale.FlashSaleID.String(),
			}
			mockFlashSale.Timestamp = time.Now().Unix()
			// Remove ObjectID because this is not passed from gateway
			mockID := mockFlashSale.ID
			mockFlashSale.ID = objectid.NilObjectID

			mockFlashSale.Timestamp = 1234
			// Can't search directly in Mongo if using embedded-arrays
			mockUpdateFlashSale := *mockFlashSale
			mockUpdateFlashSale.ID = objectid.NilObjectID
			mockUpdateFlashSale.Items = []flashSale.SoldItem{}
			mockUpdateFlashSale.FlashSaleID = uuuid.UUID{}

			update := map[string]interface{}{
				"filter": filterFlashSale,
				"update": &mockUpdateFlashSale,
			}
			marshalUpdate, err := json.Marshal(update)
			Expect(err).ToNot(HaveOccurred())
			// Reassign back ID so we can compare easily with database-entry
			mockFlashSale.ID = mockID

			Byf("Creating update MockEvent")
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.EventAction = "update"
			mockEvent.Data = marshalUpdate
			mockEvent.NanoTime = time.Now().UnixNano()
			mockEvent.UUID = uuid

			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "aggflashSale.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.Document{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.UUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.UUID))

					result := map[string]int{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					if result["matchedCount"] != 0 && result["modifiedCount"] != 0 {
						Expect(result["matchedCount"]).To(Equal(1))
						Expect(result["modifiedCount"]).To(Equal(1))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())

			mockUpdateFlashSaleFind := *mockFlashSale
			mockUpdateFlashSaleFind.Items = []flashSale.SoldItem{}
			findResult, err := aggColl.FindOne(mockUpdateFlashSaleFind)
			Expect(err).ToNot(HaveOccurred())
			findFlashSale, assertOK := findResult.(*flashSale.FlashSale)
			Expect(assertOK).To(BeTrue())
			Expect(findFlashSale).To(Equal(mockFlashSale))

			close(done)
		}, 20)

		It("should delete record", func(done Done) {
			Byf("Creating delete args")
			deleteArgs := map[string]interface{}{
				"flashSaleID": mockFlashSale.FlashSaleID,
			}
			marshalDelete, err := json.Marshal(deleteArgs)
			Expect(err).ToNot(HaveOccurred())

			Byf("Creating delete MockEvent")
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.EventAction = "delete"
			mockEvent.Data = marshalDelete
			mockEvent.NanoTime = time.Now().UnixNano()
			mockEvent.UUID = uuid

			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "aggflashSale.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.Document{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.UUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.UUID))

					result := map[string]int{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					if result["deletedCount"] != 0 {
						Expect(result["deletedCount"]).To(Equal(1))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())
			_, err = aggColl.FindOne(mockFlashSale)
			Expect(err).To(HaveOccurred())

			close(done)
		}, 20)
	})
})
