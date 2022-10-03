package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"google.golang.org/protobuf/proto"
)

var (
	bootstrapServers string = "localhost:9092"
	topicName        string = "example-06"
)

func main() {

	version, err := sarama.ParseKafkaVersion("2.1.1")
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(strings.Split(bootstrapServers, ","), config)

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():

			person := Person{}
			err := proto.Unmarshal(msg.Value, &person)

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error deserializing message: %s\n", err)
				os.Exit(1)
			}

			fmt.Printf("🧑🏻‍💻 userName: %s, favoriteNumber: %d, interests: %s\n",
				person.UserName, *person.FavoriteNumber, person.Interests)

			consumed++

		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)

}
