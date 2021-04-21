package mq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_StartStop(t *testing.T) {
	assert.NoError(t, Start())
	Stop()
	assert.NoError(t, Start())
	Stop()
}

func Test_Dispatch(t *testing.T) {
	res := map[string]string{}
	m := map[string]func([]byte) error{
		"subj1": func(msg []byte) error { res["subj1"] = string(msg); return nil },
		"subj2": func(msg []byte) error { res["subj2"] = string(msg); return nil },
	}
	assert.NoError(t, Start())
	assert.NoError(t, StartDispatcher(m))
	assert.NoError(t, Publish("subj1", []byte("msg1")))
	assert.NoError(t, Publish("subj2", []byte("msg2")))
	time.Sleep(time.Millisecond)
	assert.Equal(t, map[string]string{"subj1": "msg1", "subj2": "msg2"}, res)
	Stop()
}

func Test_Publish(t *testing.T) {
	assert.NoError(t, Start())
	assert.NoError(t, Publish("subj1", []byte("msg1")))
	assert.NoError(t, Publish("subj2", []byte("msg2")))
	Stop()
}
