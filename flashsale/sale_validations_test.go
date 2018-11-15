package flashsale

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestFlashSale only tests basic pre-processing error-checks for Aggregate functions.
func TestFlashSale(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FlashSaleAggregate Suite")
}

var _ = Describe("FlashSaleAggregate", func() {
	Describe("delete", func() {
		It("should return error if filter is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   AggregateID,
				Data:          []byte("{}"),
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Delete(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})
	})

	Describe("insert", func() {
		var flashSale *FlashSale

		BeforeEach(func() {
			flashSaleID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			flashSale = &FlashSale{
				FlashSaleID: flashSaleID,
				Items: []SoldItem{
					SoldItem{
						ItemID: itemID,
						UPC:    "test-upc",
						Weight: 12.24,
						Lot:    "test-lot",
						SKU:    "test-sku",
					},
				},
				Timestamp: time.Now().Unix(),
			}
		})

		It("should return error if flashSaleID is empty", func() {
			flashSale.FlashSaleID = uuuid.UUID{}
			marshalFlashSale, err := json.Marshal(flashSale)
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "insert",
				CorrelationID: cid,
				AggregateID:   1,
				Data:          marshalFlashSale,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if items is empty", func() {
			flashSale.Items = []SoldItem{}
			marshalFlashSale, err := json.Marshal(flashSale)
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "insert",
				CorrelationID: cid,
				AggregateID:   1,
				Data:          marshalFlashSale,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if timestamp is empty", func() {
			flashSale.Timestamp = 0
			marshalFlashSale, err := json.Marshal(flashSale)
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "insert",
				CorrelationID: cid,
				AggregateID:   1,
				Data:          marshalFlashSale,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})
	})

	Describe("update", func() {
		It("should return error if filter is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			updateArgs := map[string]interface{}{
				"filter": map[string]interface{}{},
				"update": map[string]interface{}{},
			}
			marshalArgs, err := json.Marshal(updateArgs)
			Expect(err).ToNot(HaveOccurred())
			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   AggregateID,
				Data:          marshalArgs,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Update(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if update is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			updateArgs := map[string]interface{}{
				"filter": map[string]interface{}{
					"x": 1,
				},
				"update": map[string]interface{}{},
			}
			marshalArgs, err := json.Marshal(updateArgs)
			Expect(err).ToNot(HaveOccurred())
			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   AggregateID,
				Data:          marshalArgs,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Update(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if flashSaleID in update is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			updateArgs := map[string]interface{}{
				"filter": map[string]interface{}{
					"x": 1,
				},
				"update": map[string]interface{}{
					"flashSaleID": (uuuid.UUID{}).String(),
				},
			}
			marshalArgs, err := json.Marshal(updateArgs)
			Expect(err).ToNot(HaveOccurred())
			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   AggregateID,
				Data:          marshalArgs,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Update(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if timestamp in update is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			updateArgs := map[string]interface{}{
				"filter": map[string]interface{}{
					"x": 1,
				},
				"update": map[string]interface{}{
					"timestamp": 0,
				},
			}
			marshalArgs, err := json.Marshal(updateArgs)
			Expect(err).ToNot(HaveOccurred())
			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   AggregateID,
				Data:          marshalArgs,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Update(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})
	})
})
