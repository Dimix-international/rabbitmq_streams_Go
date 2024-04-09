package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

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

	if err := ch.Qos(50, 0, false); err != nil {
		panic(err)
	}
	//Stream core
	//!!! Auto ACK has to be FALSE !!
	stream, err := ch.Consume("events", "events_consumer", false, false, false, false, amqp.Table{
		"x-stream-offset": "last", //с какого сообщения начинаем читать - можно написать цифрой,
		//"last" - выведет последнию чанку, можно "first", "5m" - 5 минутным интервалом до начальной точки
	})
	if err != nil {
		panic(err)
	}

	//Loop forever jusr read the message
	for event := range stream {
		fmt.Printf("Event: %s\n", event.CorrelationId)
		fmt.Printf("Header: %v\n", event.Headers)
		//payload
		fmt.Printf("Data: %v\n", string(event.Body))
	}
}
