package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func main() {
	// get kafka reader using environment variables.
	//kafkaURL := os.Getenv("localhost:9092")
	//topic := os.Getenv("test212")
	//groupID := os.Getenv("groupID")

	reader := getKafkaReader("localhost:9092", "test212", "groupID")

	defer reader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
