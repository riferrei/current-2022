package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

var (
	bootstrapServers string = "localhost:9092"
	topicName        string = "example-02"
)

func main() {

	if os.Args[1] == "w" {
		write()
	}

	if os.Args[1] == "r" {
		read()
	}

}

func write() {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error == nil {
					fmt.Printf("➡️ Message sent successfully to topic [%s] ✅\n", *m.TopicPartition.Topic)
				}
			case kafka.Error:
				fmt.Printf("Error: %v\n", ev)
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	var keyValue uint16 = 1
	keyBuffer := new(bytes.Buffer)
	binary.Write(keyBuffer, binary.LittleEndian, keyValue)

	myMessage := Person{
		UserName:       "Ricardo",
		FavoriteNumber: 14,
		Interests:      []string{"Marvel"},
	}

	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	err = encoder.Encode(myMessage)
	if err != nil {
		log.Fatal(err)
	}

	err = producer.Produce(&kafka.Message{
		Key:            keyBuffer.Bytes(),
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
		Value:          buffer.Bytes(),
	}, nil)

	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrQueueFull {
			time.Sleep(time.Second)
		}
		fmt.Printf("Failed to produce message: %v\n", err)
	}

	for producer.Flush(10000) > 0 {
		fmt.Print("Still waiting to flush outstanding messages\n", err)
	}

	producer.Close()

}

func read() {

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
				bytes := bytes.Buffer{}
				bytes.Write(e.Value)
				decoder := gob.NewDecoder(&bytes)
				err = decoder.Decode(&person)

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
