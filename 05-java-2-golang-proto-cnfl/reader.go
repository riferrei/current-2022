package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"github.com/google/uuid"
)

var (
	bootstrapServers  string = "localhost:9092"
	schemaRegistryURL string = "http://localhost:8081"
	topicName         string = "example-05"
)

func main() {

	sigchan := make(chan os.Signal, 1)
	topics := []string{topicName}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"broker.address.family":    "v4",
		"group.id":                 uuid.New(),
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))

	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	deser, err := protobuf.NewDeserializer(client, serde.ValueSerde, protobuf.NewDeserializerConfig())

	if err != nil {
		fmt.Printf("Failed to create deserializer: %s\n", err)
		os.Exit(1)
	}

	deser.ProtoRegistry.RegisterMessage((&Person{}).ProtoReflect().Type())

	c.SubscribeTopics(topics, nil)

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:

				value, err := deser.Deserialize(*e.TopicPartition.Topic, e.Value)

				if err != nil {
					fmt.Fprintf(os.Stderr, "Error deserializing message: %s\n", err)
					os.Exit(1)
				}

				person := value.(*Person)

				fmt.Printf("🧑🏻‍💻 userName: %s, favoriteNumber: %d, interests: %s\n",
					person.UserName, *person.FavoriteNumber, person.Interests)

				_, err = c.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n",
						e.TopicPartition)
				}

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			}
		}
	}

	fmt.Printf("Closing consumer\n")

	c.Close()

}
