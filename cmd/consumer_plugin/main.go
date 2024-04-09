package main

import (
	"fmt"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func main() {
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))

	if err != nil {
		panic(err)
	}

	//Create consumer options, we set x-stream-offset
	consumerOptions := stream.NewConsumerOptions().
		SetConsumerName("consumer_1").
		SetOffset(stream.OffsetSpecification{}.First())

	//start consumer
	consumer, err := env.NewConsumer("events", messageHandler, consumerOptions)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

}

func messageHandler(consumerContext stream.ConsumerContext, message *amqp.Message) {
	fmt.Println(message.Properties.CorrelationID)
	fmt.Println(string(message.GetData()))
}
