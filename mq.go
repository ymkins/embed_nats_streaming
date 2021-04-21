package mq

import (
	"fmt"
	"log"
	"sync"

	natsd "github.com/nats-io/nats-server/v2/server"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/stan.go"
)

const (
	clusterID    = "embed-cluster"
	dispatcherID = "embed-dispatcher"
	publisherID  = "embed-publisher"
)

var state = new(struct {
	sync.Mutex

	connPub    stan.Conn
	connSub    stan.Conn
	stanServer *stand.StanServer
})

var ErrNATS = fmt.Errorf("nats error")

func Start() error {
	state.Lock()
	defer state.Unlock()

	if state.stanServer != nil {
		log.Println("stanServer already runned:", state.stanServer.ClientURL())
		return nil
	}

	natsOpts := stand.DefaultNatsServerOptions.Clone()
	natsOpts.Port = natsd.RANDOM_PORT

	stanOpts := stand.GetDefaultOptions().Clone()
	stanOpts.ID = clusterID
	stanOpts.StoreType = stores.TypeMemory

	if stanServer, err := stand.RunServerWithOpts(stanOpts, natsOpts); err == nil {
		state.stanServer = stanServer
		log.Println("stanServer started:", state.stanServer.ClientURL())
	} else {
		log.Println("stanServer RunServerWithOpts failed:", err)
		return err
	}
	return nil
}

func Stop() {
	state.Lock()
	defer state.Unlock()

	if state.connPub != nil {
		_ = state.connPub.Close()
		state.connPub = nil
	}
	if state.connSub != nil {
		_ = state.connSub.Close()
		state.connSub = nil
	}
	if state.stanServer != nil {
		state.stanServer.Shutdown()
		state.stanServer = nil
	}
}

func Publish(subject string, msg []byte) error {
	state.Lock()
	defer state.Unlock()

	if state.connPub == nil {
		if state.stanServer == nil {
			err := fmt.Errorf("%v: unavailable", ErrNATS)
			log.Println("publisher failed:", err)
			return err
		}
		var err error
		if state.connPub, err = stan.Connect(
			clusterID,
			publisherID,
			stan.NatsURL(state.stanServer.ClientURL()),
			stan.SetConnectionLostHandler(func(c stan.Conn, e error) {
				log.Println("publisher ConnectionLostHandler:", e.Error())
				state.Lock()
				_ = c.Close()
				state.connPub = nil
				state.Unlock()
			}),
		); err != nil {
			log.Println("publisher failed to connect:", err)
			return err
		}
	}
	return state.connPub.Publish(subject, msg)
}

func StartDispatcher(m map[string]func([]byte) error) error {
	state.Lock()
	defer state.Unlock()

	if state.connSub == nil {
		if state.stanServer == nil {
			err := fmt.Errorf("%v: unavailable", ErrNATS)
			log.Println("dispatcher failed:", err)
			return err
		}
		var err error
		if state.connSub, err = stan.Connect(
			clusterID,
			dispatcherID,
			stan.NatsURL(state.stanServer.ClientURL()),
			stan.SetConnectionLostHandler(func(c stan.Conn, e error) {
				log.Println("dispatcher ConnectionLostHandler:", e.Error())
				state.Lock()
				_ = c.Close()
				state.connSub = nil
				state.Unlock()
			}),
		); err != nil {
			log.Println("dispatcher failed to connect:", err)
			return err
		}
	}

	for subj, handler := range m {
		if err := openDurable(subj, handler); err != nil {
			return err
		}
	}
	return nil
}

func openDurable(subj string, handler func([]byte) error) error {
	_, err := state.connSub.Subscribe(
		subj,
		func(msg *stan.Msg) {
			if err := handler(msg.Data); err != nil {
				log.Println("msg failed")
				return
			}
			log.Println("msg ok")
		},
		stan.StartWithLastReceived(),
	)
	return err
}
