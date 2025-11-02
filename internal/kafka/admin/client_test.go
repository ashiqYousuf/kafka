package admin

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/ashiqYousuf/kafka/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockKafkaAdminClient struct {
	mock.Mock
}

func init() {
	logger.LoggerInit()
}

func (m *MockKafkaAdminClient) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	args := m.Called(topic, detail, validateOnly)
	return args.Error(0)
}

func (m *MockKafkaAdminClient) ListTopics() (map[string]sarama.TopicDetail, error) {
	args := m.Called()
	if err := args.Error(1); err != nil {
		return nil, err
	}
	return args.Get(0).(map[string]sarama.TopicDetail), args.Error(1)
}

func (m *MockKafkaAdminClient) DeleteTopic(topic string) error {
	args := m.Called(topic)
	return args.Error(0)
}

func (m *MockKafkaAdminClient) AlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {
	args := m.Called(resourceType, name, entries, validateOnly)
	return args.Error(0)
}

func (m *MockKafkaAdminClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestCreateTopic(t *testing.T) {
	ctx := context.Background()
	type test struct {
		Name       string
		topic      *Topic
		setUpMocks func(client *MockKafkaAdminClient)
		wantErr    error
	}

	tests := []test{
		{
			Name:  "CreateTopic_Success",
			topic: getTopic("new_topic", 5, 2),
			setUpMocks: func(client *MockKafkaAdminClient) {
				client.On("CreateTopic", "new_topic", mock.MatchedBy(func(d *sarama.TopicDetail) bool {
					return d.NumPartitions == 5 && d.ReplicationFactor == 2
				}), false).
					Return(nil)
			},
			wantErr: nil,
		},
		{
			Name:  "CreateTopic_Failure",
			topic: getTopic("new_topic", 5, 2),
			setUpMocks: func(client *MockKafkaAdminClient) {
				client.On("CreateTopic", "new_topic", mock.MatchedBy(func(d *sarama.TopicDetail) bool {
					return d.NumPartitions == 5 && d.ReplicationFactor == 2
				}), false).
					Return(errors.New("internal error"))
			},
			wantErr: errors.New("internal error"),
		},
	}

	for _, tc := range tests {
		client := new(MockKafkaAdminClient)
		SetKafkaAdminClient(client)
		tc.setUpMocks(client)

		invokeClient := GetKafkaAdminClient()
		t.Run(tc.Name, func(t *testing.T) {
			err := invokeClient.CreateTopic(ctx, tc.topic)
			if err != nil {
				assert.EqualError(t, err, tc.wantErr.Error(), "got %v want %v", err, tc.wantErr)
			}
		})
	}
}

func TestListTopics(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		setUpMocks func(client *MockKafkaAdminClient)
		wantErr    error
	}{
		{
			name: "ListTopics_Success",
			setUpMocks: func(client *MockKafkaAdminClient) {
				topics := map[string]sarama.TopicDetail{
					"topic1": {NumPartitions: 3, ReplicationFactor: 2},
				}
				client.On("ListTopics").Return(topics, nil)
			},
			wantErr: nil,
		},
		{
			name: "ListTopics_Failure",
			setUpMocks: func(client *MockKafkaAdminClient) {
				client.On("ListTopics").Return(nil, errors.New("failed to list"))
			},
			wantErr: errors.New("failed to list"),
		},
	}

	for _, tc := range tests {
		client := new(MockKafkaAdminClient)
		SetKafkaAdminClient(client)
		tc.setUpMocks(client)

		invokeClient := GetKafkaAdminClient()
		t.Run(tc.name, func(t *testing.T) {
			_, err := invokeClient.ListTopics(ctx)
			if tc.wantErr != nil {
				assert.EqualError(t, err, tc.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDeleteTopic(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		topic      string
		setUpMocks func(client *MockKafkaAdminClient)
		wantErr    error
	}{
		{
			name:  "DeleteTopic_Success",
			topic: "topicA",
			setUpMocks: func(client *MockKafkaAdminClient) {
				client.On("DeleteTopic", "topicA").Return(nil)
			},
			wantErr: nil,
		},
		{
			name:  "DeleteTopic_Failure",
			topic: "topicB",
			setUpMocks: func(client *MockKafkaAdminClient) {
				client.On("DeleteTopic", "topicB").Return(errors.New("delete failed"))
			},
			wantErr: errors.New("delete failed"),
		},
	}

	for _, tc := range tests {
		client := new(MockKafkaAdminClient)
		SetKafkaAdminClient(client)
		tc.setUpMocks(client)

		invokeClient := GetKafkaAdminClient()
		t.Run(tc.name, func(t *testing.T) {
			err := invokeClient.DeleteTopic(ctx, tc.topic)
			if tc.wantErr != nil {
				assert.EqualError(t, err, tc.wantErr.Error(), "got %v want %v", err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAlterTopicConfig(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		topic      *Topic
		setUpMocks func(client *MockKafkaAdminClient)
		wantErr    error
	}{
		{
			name: "AlterTopicConfig_Success",
			topic: &Topic{
				Name: "topicB",
				ExtraParams: map[string]string{
					"cleanup.policy": "compact",
				},
			},
			setUpMocks: func(client *MockKafkaAdminClient) {
				client.On("AlterConfig", sarama.TopicResource, "topicB",
					mock.MatchedBy(func(m map[string]*string) bool {
						return *m["cleanup.policy"] == "compact"
					}),
					false,
				).Return(nil)
			},
			wantErr: nil,
		},
		{
			name: "AlterTopicConfig_Failure",
			topic: &Topic{
				Name: "topicB",
				ExtraParams: map[string]string{
					"retention.ms": "10000",
				},
			},
			setUpMocks: func(client *MockKafkaAdminClient) {
				client.On("AlterConfig", sarama.TopicResource, "topicB",
					mock.MatchedBy(func(m map[string]*string) bool {
						return *m["retention.ms"] == "10000"
					}),
					false,
				).Return(errors.New("update failed"))
			},
			wantErr: errors.New("update failed"),
		},
	}

	for _, tc := range tests {
		client := new(MockKafkaAdminClient)
		SetKafkaAdminClient(client)
		tc.setUpMocks(client)

		invokeClient := GetKafkaAdminClient()
		t.Run(tc.name, func(t *testing.T) {
			err := invokeClient.AlterTopicConfig(ctx, tc.topic)
			if tc.wantErr != nil {
				assert.EqualError(t, err, tc.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func getTopic(name string, nP int, rF int) *Topic {
	return &Topic{
		Name:              name,
		NumPartitions:     nP,
		ReplicationFactor: rF,
	}
}

func getSaramaTopic(nP int, rF int) *sarama.TopicDetail {
	return &sarama.TopicDetail{
		NumPartitions:     int32(nP),
		ReplicationFactor: int16(rF),
	}
}
