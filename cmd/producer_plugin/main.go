package main

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const (
	EventStreamName = "events"
)

type Event struct {
	Name string
}

func main() {
	//connect to the stream plugin
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))

	if err != nil {
		panic(err)
	}

	//declare the stream, set segmentsize and Maxbytes on stream
	err = env.DeclareStream(EventStreamName, stream.NewStreamOptions().
		SetMaxSegmentSizeBytes(stream.ByteCapacity{}.MB(1)).
		SetMaxLengthBytes(stream.ByteCapacity{}.MB(2)))
	if err != nil {
		panic(err)
	}

	//create producer
	producerOptions := stream.NewProducerOptions()
	producerOptions.SetProducerName("producer")

	//Batch 100 events in the frame, and the SDK will handle everything - но включение этих параметров
	// отключает возможность контроля отправки сообщений которые уже были отправлены
	//producerOptions.SetSubEntrySize(100)
	//producerOptions.SetCompression(stream.Compression{}.Gzip())

	producer, err := env.NewProducer(EventStreamName, producerOptions)
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	for i := 0; i <= 6001; i++ {
		event := Event{
			Name: "Test",
		}

		data, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}

		message := amqp.NewMessage(data)
		//apply properties to our message
		props := &amqp.MessageProperties{
			CorrelationID: uuid.NewString(),
		}

		message.Properties = props

		//set publishing id to prevent deduplication
		message.SetPublishingId(int64(i))

		//sending the message
		if err := producer.Send(message); err != nil {
			panic(err)
		}
	}
}
