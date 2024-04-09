package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Event struct {
	Name string
}

func main() {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", "guest", "guest", "localhost:5672", ""))
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	//Stream core
	//create new queue
	q, err := ch.QueueDeclare("events", true, false, false, true, amqp.Table{
		"x-queue-type":                    "stream", //тип очереди
		"x-stream-max-segment-size-bytes": 30000,    //each segment file is allowed 0.03 MB,
		"x-max-length-bytes":              150000,   //total stream size 0.15 MB
		//"x-max-age": "10m", //время жизни
	})
	if err != nil {
		panic(err)
	}

	//publish 1001 messages
	ctx := context.Background()
	for i := 0; i <= 1000; i++ {
		event := Event{
			Name: "Test",
		}

		data, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}

		err = ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
			Body:          data,
			CorrelationId: uuid.NewString(),
		})
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(q.Name)
}
