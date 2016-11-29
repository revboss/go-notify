package notify

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"reflect"
	"time"
)

type Schema struct {
	Type    string      `json:"type"`
	Version int         `json:"version"`
	Schema  interface{} `json:"schema"`
}

type SQS interface {
	DeleteMessage(*sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
	ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	SendMessage(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error)
}

type Notification struct {
	Time    time.Time   `json:"time"`
	Type    string      `json:"type"`
	Version int         `json:"version"`
	Data    interface{} `json:"notification"`
}

type Notifications struct {
	SQS      SQS
	QueueURL string
	Rate     time.Duration
	Schemas  map[string]map[int]Schema

	ch chan interface{}
}

func New(sqs SQS, queue string) *Notifications {
	notifications := &Notifications{
		QueueURL: queue,
		Schemas:  make(map[string]map[int]Schema),
		Rate:     1 * time.Second,
		SQS:      sqs,

		ch: make(chan interface{}),
	}

	return notifications
}

func (n Notifications) AddSchema(schema Schema) error {
	if schemas, ok := n.Schemas[schema.Type]; ok {
		if _, ok := schemas[schema.Version]; ok {
			return fmt.Errorf("Schema already exists: %s:%d", schema.Type, schema.Version)
		}
	} else {
		n.Schemas[schema.Type] = make(map[int]Schema)
	}

	n.Schemas[schema.Type][schema.Version] = schema

	return nil
}

func (n Notifications) Receive(notification *Notification) error {
	data, err := n.receive()
	if err != nil {
		return err
	}

	*notification = data
	return nil
}

func (n Notifications) receive() (Notification, error) {
	var notifications *sqs.ReceiveMessageOutput
	var err error

	for {
		time.Sleep(n.Rate)
		notifications, err = n.SQS.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl: aws.String(n.QueueURL),
		})

		// HANDLE TIMEOUT ERRORS HERE SEPARATELY FROM OTHER ERRORS SINCE
		// WE MAY SWITCH TO LONG POLLING (WHICH CAN TIMEOUT OFTEN) SO
		// THAT WE CAN DECREASE IDLE LOOPING HERE. THE ACTION TO TAKE ON
		// TIMEOUT IS TO CALL `continue`.

		if err != nil {
			return Notification{}, err
		}

		//THE FOLLOWING WOULD BECOME OBSOLETE WITH LONG POLLING SINCE WE
		//ARE GUARANTEED TO HAVE AT LEAST ONE MESSAGE.

		if len(notifications.Messages) == 0 {
			continue
		}

		break
	}

	data, err := n.handle(notifications)

	if err != nil {
		return Notification{}, err
	}

	return data, nil
}

func (n Notifications) handle(notifications *sqs.ReceiveMessageOutput) (Notification, error) {
	notification := Notification{}

	//We only handle only one message at a time all remaining messages get
	//put back on the queue for later retrieval this can be optimized in the
	//future (some of our queues have a max message retrieval of 1 anyway).

	message := notifications.Messages[0]

	e := json.Unmarshal([]byte(*message.Body), &notification)
	if e != nil {
		return Notification{}, e
	}

	schema, ok := n.Schemas[notification.Type][notification.Version]
	if !ok {
		return Notification{}, fmt.Errorf("Schema does not exist: %s:%d", notification.Type, notification.Version)
	}

	data, e := base64.StdEncoding.DecodeString(notification.Data.(string))
	if e != nil {
		return Notification{}, e
	}

	re := reflect.New(reflect.TypeOf(schema.Schema)).Interface()

	e = json.Unmarshal(data, re)
	if e != nil {
		return Notification{}, e
	}

	notification.Data = re

	_, e = n.SQS.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(n.QueueURL),
		ReceiptHandle: message.ReceiptHandle,
	})
	if e != nil {
		return Notification{}, e
	}

	return notification, nil
}

func (n Notifications) Send(notification Notification) error {
	schema, ok := n.Schemas[notification.Type][notification.Version]
	if !ok {
		return fmt.Errorf("Schema does not exist: %s:%d", notification.Type, notification.Version)
	}

	notification.Time = time.Now()

	nd, e := json.Marshal(notification.Data)
	if e != nil {
		return e
	}

	sc := schema.Schema

	e = json.Unmarshal(nd, &sc)
	if e != nil {
		return e
	}

	scj, e := json.Marshal(sc)
	if e != nil {
		return e
	}

	notification.Data = base64.StdEncoding.EncodeToString(scj)

	body, e := json.Marshal(notification)
	if e != nil {
		return e
	}

	_, e = n.SQS.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(body)),
		QueueUrl:    aws.String(n.QueueURL),
	})

	return e
}
