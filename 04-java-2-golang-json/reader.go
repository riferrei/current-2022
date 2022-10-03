package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

var (
	bootstrapServers string = "localhost:9092"
	topicName        string = "example-04"
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

				person := Person{}
				err = json.Unmarshal(e.Value, &person)

				if err != nil {
					fmt.Fprintf(os.Stderr, "Error deserializing message: %s\n", err)
					os.Exit(1)
				}
				fmt.Printf("🧑🏻‍💻 userName: %s, favoriteNumber: %d, interests: %s\n",
					person.UserName, person.FavoriteNumber, person.Interests)

				_, err := c.StoreMessage(e)
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
