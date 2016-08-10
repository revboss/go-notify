package notify_test

import (
	. "github.com/franela/goblin"
	"github.com/revboss/go-mock"
	"github.com/revboss/go-notify"
	"testing"
	"time"
)

var TestQueue = &mock.SQS{}

type TestData struct {
	String string
	Int    int
	Bool   bool
}

func TestNotify(t *testing.T) {
	g := Goblin(t)

	g.Describe("Notifications", func() {
		g.Describe("New", func() {
			g.It("Should create a new notify.Notifications", func() {
				notifications := notify.New(TestQueue, "test-queue")
				g.Assert(notifications != nil)
			})
		})

		g.Describe("AddSchema", func() {
			g.It("Should add a new schema version", func() {
				notifications := notify.New(TestQueue, "test-queue")

				e := notifications.AddSchema(notify.Schema{
					Type:    "testing",
					Version: 1,
					Schema:  TestData{},
				})
				if e != nil {
					t.Error(e)
					t.FailNow()
				}
			})

			g.It("Should not be able to define a schema version twice", func() {
				notifications := notify.New(TestQueue, "test-queue")

				e := notifications.AddSchema(notify.Schema{
					Type:    "testing",
					Version: 1,
					Schema:  TestData{},
				})
				if e != nil {
					t.Error(e)
					t.FailNow()
				}

				e = notifications.AddSchema(notify.Schema{
					Type:    "testing",
					Version: 1,
					Schema:  TestData{},
				})

				g.Assert(e != nil)
			})
		})

		g.Describe("Send", func() {
			g.It("Should send a notification", func() {
				count := 0

				notifications := notify.New(TestQueue, "test-queue")
				notifications.Rate = 1 * time.Second

				go func() {
					var notification notify.Notification
					for {
						e := notifications.Receive(&notification)
						g.Assert(e).Equal(nil)

						g.Assert(notification.Type).Equal("testing")

						data := notification.Data.(*TestData)
						g.Assert(data.String).Equal("string")

						count++
					}
				}()

				e := notifications.AddSchema(notify.Schema{
					Type:    "testing",
					Version: 1,
					Schema:  TestData{},
				})
				g.Assert(e).Equal(nil)

				e = notifications.Send(notify.Notification{
					Type:    "testing",
					Version: 1,
					Data: TestData{
						String: "string",
						Int:    1,
						Bool:   true,
					},
				})
				g.Assert(e).Equal(nil)

				time.Sleep(2 * time.Second)

				g.Assert(count > 0).IsTrue()
			})

			g.It("Should not be able to send a notification without a schema", func() {
				notifications := notify.New(TestQueue, "test-queue")

				e := notifications.Send(notify.Notification{
					Type:    "testing",
					Version: 1,
					Data: TestData{
						String: "string",
						Int:    1,
						Bool:   true,
					},
				})
				g.Assert(e != nil)
			})
		})
	})
}
