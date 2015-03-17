package gaeamqp

import (
	"net"
	"time"

	"github.com/streadway/amqp"

	"appengine"
	"appengine/socket"
)

const (
	defaultHeartbeat         = 10 * time.Second
	defaultConnectionTimeout = 30 * time.Second
)

func Dial(c appengine.Context, url string) (*amqp.Connection, error) {
	return amqp.DialConfig(url, amqp.Config{
		Heartbeat: defaultHeartbeat,
		Dial: func(network, addr string) (net.Conn, error) {
			conn, err := socket.DialTimeout(c, network, addr, defaultConnectionTimeout)
			if err != nil {
				return nil, err
			}

			if err := conn.SetReadDeadline(time.Now().Add(defaultConnectionTimeout)); err != nil {
				return nil, err
			}

			return conn, nil
		},
	})
}
