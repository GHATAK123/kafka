package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := []string{"localhost:29092"}
	topic := "SHARED_KAFKA_TOPIC"

	err := CreateTopic(brokers, topic)
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println("Topic Created Successfully")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("Key-A"),
		Value: []byte("Hello from Kafka 5"),
	})

	if err != nil {
		log.Fatal(err)
		return
	}

	fmt.Println("Succesfully Published message to the Topic")

}
