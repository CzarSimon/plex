package pkg

import (
	"strconv"
	"testing"

	"github.com/CzarSimon/plex/pkg/schema"
)

const testTopicName = "test"

var testMsg = schema.NewMessage(testTopicName, []byte("msg body"))

func TestQueueBufferLen(t *testing.T) {
	limit := 3
	buffer := newQueueBuffer(uint64(limit))
	if buffer.Len() != 0 {
		t.Errorf("queueBuffer inital length is not 0. Got=%d", buffer.Len())
	}
	buffer.Put(testMsg)
	if buffer.Len() != 1 {
		t.Errorf("queueBuffer.Len wrong Expected=1. Got=%d", buffer.Len())
	}
	addMessagesInBuffer(buffer, 10)
	if buffer.Len() != uint64(limit) {
		t.Errorf("queueBuffer.Len wrong Expected=%d Got=%d", limit, buffer.Len())
	}
	buffer = newQueueBuffer(0)
	addMessagesInBuffer(buffer, 10)
	if buffer.Len() != 10 {
		t.Errorf("queueBuffer.Len wrong Expected=10 Got=%d", buffer.Len())
	}
}

func TestQueueBufferPut(t *testing.T) {
	buf := newQueueBuffer(3)
	err := buf.Put(testMsg)
	if err != nil {
		t.Errorf("queueMessage.Put unexpected error=%s", err)
	}
	expectedBody := "msg body"
	if getFirstString(buf) != expectedBody {
		t.Errorf("queueBuffer wrong first message. Expected=%s Got=%s",
			expectedBody, getFirstString(buf))
	}
	if getLastString(buf) != expectedBody {
		t.Errorf("queueBuffer wrong last message. Expected=%s Got=%s",
			expectedBody, getLastString(buf))
	}
	addMessagesInBuffer(buf, 3)
	if getFirstString(buf) != "0" {
		t.Errorf("queueBuffer wrong first message. Expected=0 Got=%s", getFirstString(buf))
	}
	if string(buf.first.next.message.Body) != "1" {
		t.Errorf("queueBuffer wrong first message. Expected=1 Got=%s",
			string(buf.first.next.message.Body))
	}
	if getLastString(buf) != "2" {
		t.Errorf("queueBuffer wrong last message. Expected=2 Got=%s", getLastString(buf))
	}
}

func TestQueueBufferGetAll(t *testing.T) {
	limit := 100
	buf := newQueueBuffer(uint64(limit))
	messages, err := buf.GetAll()
	if err != nil {
		t.Errorf("queueMessage.GetAll unexpected error=%s", err)
	}
	if len(messages) != 0 {
		t.Errorf("queueMessage.GetAll returned incorret length. Expected=0 Got=%d",
			len(messages))
	}
	addMessagesInBuffer(buf, limit)
	messages, err = buf.GetAll()
	if err != nil {
		t.Errorf("queueMessage.GetAll unexpected error=%s", err)
	}
	if len(messages) != limit {
		t.Errorf("queueMessage.GetAll returned incorret length. Expected=%d Got=%d",
			limit, len(messages))
	}
	for i := 0; i < limit; i++ {
		expectedBody := strconv.Itoa(i)
		actualBody := string(messages[i].Body)
		if actualBody != expectedBody {
			t.Errorf("%d - queueMessage.GetAll returned wrong message. Expected body=%s Got=%s",
				i, expectedBody, actualBody)
		}
	}
}

func addMessagesInBuffer(buf MessageBuffer, number int) {
	for i := 0; i < number; i++ {
		num := strconv.Itoa(i)
		buf.Put(schema.NewMessage(testTopicName, []byte(num)))
	}
}

func getFirstString(buf *queueBuffer) string {
	return string(buf.first.message.Body)
}

func getLastString(buf *queueBuffer) string {
	return string(buf.last.message.Body)
}
