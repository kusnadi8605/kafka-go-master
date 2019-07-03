package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	kafka "github.com/segmentio/kafka-go"
)

func producerHandler(kafkaWriter *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)

		if err != nil {
			log.Fatalln(err)
		}
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
			Value: body,
		}
		err = kafkaWriter.WriteMessages(req.Context(), msg)

		if err != nil {
			wrt.Write([]byte(err.Error()))
			log.Fatalln(err)
		}
	})
}

func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}

func main() {
	// get kafka writer using environment variables.

	//consumers, err := sarama.NewConsumer([]string{"kafka:9092"}, kafkaConfig)
	//kafkaURL := os.Getenv("localhost:9092")
	//topic := os.Getenv("test212")
	kafkaWriter := getKafkaWriter("localhost:9092", "test212")

	defer kafkaWriter.Close()

	// Add handle func for producer.
	http.HandleFunc("/", producerHandler(kafkaWriter))

	// Run the web server.
	fmt.Println("start producer-api ... !!")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
