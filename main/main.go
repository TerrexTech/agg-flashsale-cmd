package main

import (
	"log"
	"os"

	"github.com/TerrexTech/go-agg-framer/framer"
	"github.com/TerrexTech/go-kafkautils/kafka"

	"github.com/TerrexTech/agg-flashsale-cmd/flashsale"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventspoll/poll"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

func validateEnv() error {
	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",

		"KAFKA_CONSUMER_EVENT_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",

		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",
		"KAFKA_PRODUCER_EVENT_TOPIC",
		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",
		"KAFKA_PRODUCER_RESPONSE_TOPIC",

		"MONGO_HOSTS",
		"MONGO_DATABASE",
		"MONGO_AGG_COLLECTION",
		"MONGO_META_COLLECTION",

		"MONGO_CONNECTION_TIMEOUT_MS",
		"MONGO_RESOURCE_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		return err
	}
	return nil
}

func main() {
	log.Println("Reading environment file")
	err := godotenv.Load("./.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	err = validateEnv()
	if err != nil {
		log.Fatalln(err)
	}

	kc, err := loadKafkaConfig()
	if err != nil {
		err = errors.Wrap(err, "Error in KafkaConfig")
		log.Fatalln(err)
	}
	mc, err := loadMongoConfig()
	if err != nil {
		err = errors.Wrap(err, "Error in MongoConfig")
		log.Fatalln(err)
	}
	ioConfig := poll.IOConfig{
		ReadConfig: poll.ReadConfig{
			EnableInsert: true,
			EnableUpdate: true,
			EnableDelete: true,
		},
		KafkaConfig: *kc,
		MongoConfig: *mc,
	}

	eventPoll, err := poll.Init(ioConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating EventPoll service")
		log.Fatalln(err)
	}

	prodConfig := &kafka.ProducerConfig{
		KafkaBrokers: *commonutil.ParseHosts(os.Getenv("KAFKA_BROKERS")),
	}
	topicConfig := &framer.TopicConfig{
		DocumentTopic: os.Getenv("KAFKA_PRODUCER_RESPONSE_TOPIC"),
	}
	frm, err := framer.New(eventPoll.Context(), prodConfig, topicConfig)

	for {
		select {
		case err := <-eventPoll.Wait():
			err = errors.Wrap(err, "service-context closed")
			log.Fatalln(err)

		case eventResp := <-eventPoll.Delete():
			go func(eventResp *poll.EventResponse) {
				if eventResp == nil {
					return
				}
				err := eventResp.Error
				if err != nil {
					err = errors.Wrap(err, "Error in Delete-EventResponse")
					log.Println(err)
					return
				}
				kafkaResp := flashsale.Delete(mc.AggCollection, &eventResp.Event)
				if kafkaResp != nil {
					frm.Document <- kafkaResp
				}
			}(eventResp)

		case eventResp := <-eventPoll.Insert():
			go func(eventResp *poll.EventResponse) {
				if eventResp == nil {
					return
				}
				err := eventResp.Error
				if err != nil {
					err = errors.Wrap(eventResp.Error, "Error in Insert-EventResponse")
					log.Println(err)
					return
				}
				kafkaResp := flashsale.Insert(mc.AggCollection, &eventResp.Event)
				if kafkaResp != nil {
					frm.Document <- kafkaResp
				}
			}(eventResp)

		case eventResp := <-eventPoll.Update():
			go func(eventResp *poll.EventResponse) {
				if eventResp == nil {
					return
				}
				err := eventResp.Error
				if err != nil {
					err = errors.Wrap(err, "Error in Update-EventResponse")
					log.Println(err)
					return
				}
				kafkaResp := flashsale.Update(mc.AggCollection, &eventResp.Event)
				if kafkaResp != nil {
					frm.Document <- kafkaResp
				}
			}(eventResp)
		}
	}
}
