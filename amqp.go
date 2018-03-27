package gaeamqp

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"google.golang.org/appengine/socket"
)

const (
	defaultHeartbeat         = 10 * time.Second
	defaultConnectionTimeout = 30 * time.Second
)

func appEngineDial(c context.Context) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		conn, err := socket.DialTimeout(c, network, addr, defaultConnectionTimeout)
		if err != nil {
			return nil, err
		}

		if err := conn.SetReadDeadline(time.Now().Add(defaultConnectionTimeout)); err != nil {
			return nil, err
		}

		return conn, nil
	}
}

func Dialer(c context.Context) func(url string) (*amqp.Connection, error) {
	return func(url string) (*amqp.Connection, error) {
		return amqp.DialConfig(url, amqp.Config{
			Heartbeat: defaultHeartbeat,
			Dial:      appEngineDial(c),
		})
	}
}

func DialerTLS(c context.Context) func(url string, amqps *tls.Config) (*amqp.Connection, error) {
	return func(url string, amqps *tls.Config) (*amqp.Connection, error) {
		return amqp.DialConfig(url, amqp.Config{
			Heartbeat:       defaultHeartbeat,
			Dial:            appEngineDial(c),
			TLSClientConfig: amqps,
		})
	}
}

func Dial(c context.Context, url string) (*amqp.Connection, error) {
	return Dialer(c)(url)
}

func DialTLS(c context.Context, url string, amqps *tls.Config) (*amqp.Connection, error) {
	return DialerTLS(c)(url, amqps)
}
