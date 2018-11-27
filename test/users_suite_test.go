package test

import (
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/TerrexTech/rmb-userauth-users/connutil"

	"github.com/Shopify/sarama"
	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/rmb-userauth-users/model"
	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestEvent tests Event-handling.
func TestEvent(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"SERVICE_NAME",

		"KAFKA_BROKERS",

		"KAFKA_CONSUMER_EVENT_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",

		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",

		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",
		"KAFKA_PRODUCER_EVENT_TOPIC",

		"KAFKA_END_OF_STREAM_TOKEN",

		"MONGO_HOSTS",
		"MONGO_USERNAME",
		"MONGO_PASSWORD",

		"MONGO_DATABASE",
		"MONGO_AGG_COLLECTION",
		"MONGO_META_COLLECTION",

		"MONGO_CONNECTION_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "EventHandler Suite")
}

func mockEvent(
	input chan<- *sarama.ProducerMessage,
	action string,
	data []byte,
	topic string,
) *cmodel.Event {
	eventUUID, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())
	cid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	mockEvent := &cmodel.Event{
		AggregateID:   model.AggregateID,
		Action:        action,
		CorrelationID: cid,
		Data:          data,
		Source:        "test-source",
		NanoTime:      time.Now().UnixNano(),
		UUID:          eventUUID,
		YearBucket:    2018,
	}

	// Produce command on Kafka topic
	testEventMsg, err := json.Marshal(mockEvent)
	Expect(err).ToNot(HaveOccurred())

	input <- kafka.CreateMessage(topic, testEventMsg)
	log.Printf("====> Produced mock event: %s on topic: %s", eventUUID, topic)
	return mockEvent
}

var _ = Describe("UsersReadModel", func() {
	var (
		coll *mongo.Collection

		eventTopic string
		producer   *kafka.Producer
	)

	BeforeSuite(func() {
		eventTopic = os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")

		kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
		kafkaBrokers := *commonutil.ParseHosts(kafkaBrokersStr)

		var err error
		producer, err = kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		})
		Expect(err).ToNot(HaveOccurred())

		mc, err := connutil.LoadMongoConfig()
		Expect(err).ToNot(HaveOccurred())
		coll = mc.AggCollection
	})

	Describe("UserRegistered-Event", func() {
		It("should insert user into database", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := model.User{
				Email:     "test-email",
				FirstName: "test-firstName",
				LastName:  "test-lastName",
				UserName:  uid.String(),
				Password:  "test-password",
				Role:      "test-role",
				UserID:    uid.String(),
			}
			marshalUser, err := json.Marshal(mockUser)
			Expect(err).ToNot(HaveOccurred())

			mockEvent(producer.Input(), "UserRegistered", marshalUser, eventTopic)
			time.Sleep(5 * time.Second)

			_, err = coll.FindOne(mockUser)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Describe("UserUpdated-Event", func() {
		It("should update user in database", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := model.User{
				Email:     "test-email",
				FirstName: "test-firstName",
				LastName:  "test-lastName",
				UserName:  uid.String(),
				Password:  "test-password",
				Role:      "test-role",
				UserID:    uid.String(),
			}
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			fName, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			updateParams := map[string]interface{}{
				"filter": &model.User{
					UserID: uid.String(),
				},
				"update": &model.User{
					FirstName: fName.String(),
				},
			}
			marshalParams, err := json.Marshal(updateParams)
			Expect(err).ToNot(HaveOccurred())

			mockEvent(producer.Input(), "UserUpdated", marshalParams, eventTopic)
			time.Sleep(5 * time.Second)

			mockUser.FirstName = fName.String()
			_, err = coll.FindOne(mockUser)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Describe("UserDeleted-Event", func() {
		It("should delete user from database", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := model.User{
				Email:     "test-email",
				FirstName: "test-firstName",
				LastName:  "test-lastName",
				UserName:  uid.String(),
				Password:  "test-password",
				Role:      "test-role",
				UserID:    uid.String(),
			}
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			deleteParams := map[string]interface{}{
				"userID": uid.String(),
			}
			marshalParams, err := json.Marshal(deleteParams)
			Expect(err).ToNot(HaveOccurred())

			mockEvent(producer.Input(), "UserDeleted", marshalParams, eventTopic)
			time.Sleep(5 * time.Second)

			_, err = coll.FindOne(mockUser)
			Expect(err).To(HaveOccurred())
		})
	})
})
