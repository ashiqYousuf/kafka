package producer

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/ashiqYousuf/kafka/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockProducerClient struct {
	mock.Mock
	inputChan     chan *sarama.ProducerMessage
	successesChan chan *sarama.ProducerMessage
	errorsChan    chan *sarama.ProducerError
}

func (m *MockProducerClient) Input() chan<- *sarama.ProducerMessage {
	// we don't need to call Called as they are accessors of channels
	return m.inputChan
}

func (m *MockProducerClient) Successes() <-chan *sarama.ProducerMessage {
	return m.successesChan
}

func (m *MockProducerClient) Errors() <-chan *sarama.ProducerError {
	return m.errorsChan
}

func (m *MockProducerClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func init() {
	logger.LoggerInit()
}

func TestKafkaProducer_Send_Success(t *testing.T) {
	client := new(MockProducerClient)
	client.inputChan = make(chan *sarama.ProducerMessage, 1)

	SetProducerClient(client)
	invokeClient := GetProducerClient()

	ctx := context.Background()

	topic := "topic1"
	key := "key1"
	value := "value1: a random message"
	invokeClient.Send(ctx, topic, key, []byte(value))
	select {
	case msg := <-client.inputChan:
		assert.Equal(t, topic, msg.Topic)
		keyBytes, _ := msg.Key.Encode()
		valBytes, _ := msg.Value.Encode()
		assert.Equal(t, key, string(keyBytes))
		assert.Equal(t, string(value), string(valBytes))
	case <-time.After(500 * time.Millisecond):
		t.Fatal("message was not sent to Input channel")
	}
}

func TestKafkaProducer_Send_Failure(t *testing.T) {
	client := new(MockProducerClient)
	client.inputChan = make(chan *sarama.ProducerMessage, 1)

	SetProducerClient(client)
	invokeClient := GetProducerClient()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	topic := "topic1"
	key := "key1"
	value := "value1: a random message"

	done := make(chan struct{})
	go func() {
		invokeClient.Send(ctx, topic, key, []byte(value))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("message was not sent to Input channel")
	}
}
