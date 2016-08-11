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
	go n.receive()

	data, ok := <-n.ch
	if !ok {
		return fmt.Errorf("Receive channel unexpectedly closed.")
	}

	switch data.(type) {
	case Notification:
		*notification = data.(Notification)
		return nil

	case error:
		return data.(error)
	}

	return fmt.Errorf("Got unexpected data on Receive channel: %+v\n", data)
}

func (n Notifications) receive() {
	for {
		time.Sleep(n.Rate)
		notifications, e := n.SQS.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl: aws.String(n.QueueURL),
		})
		if e != nil {
			n.ch <- e
			continue
		}

		if len(notifications.Messages) == 0 {
			continue
		}

		n.handle(notifications)
	}
}

func (n Notifications) handle(notifications *sqs.ReceiveMessageOutput) {
	for _, message := range notifications.Messages {
		notification := Notification{}

		e := json.Unmarshal([]byte(*message.Body), &notification)
		if e != nil {
			n.ch <- e
			continue
		}

		schema, ok := n.Schemas[notification.Type][notification.Version]
		if !ok {
			n.ch <- fmt.Errorf("Schema does not exist: %s:%d", notification.Type, notification.Version)
			continue
		}

		data, e := base64.StdEncoding.DecodeString(notification.Data.(string))
		if e != nil {
			n.ch <- e
			continue
		}

		re := reflect.New(reflect.TypeOf(schema.Schema)).Interface()

		e = json.Unmarshal(data, re)
		if e != nil {
			n.ch <- e
			continue
		}

		notification.Data = re

		_, e = n.SQS.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(n.QueueURL),
			ReceiptHandle: message.ReceiptHandle,
		})
		if e != nil {
			n.ch <- e
			continue
		}

		n.ch <- notification
	}
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
