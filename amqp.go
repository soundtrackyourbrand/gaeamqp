package gaeamqp

import (
	"fmt"
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

func dial(c appengine.Context, url string) (*amqp.Connection, error) {
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

type context interface {
	appengine.Context

	GetAMQPAuthority() string
}

func Publish(c context, message string) (err error) {
	conn, err := dial(c, fmt.Sprintf("amqp://%s/", c.GetAMQPAuthority()))
	if err != nil {
		c.Errorf("Failed to connect to RabbitMQ")
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		c.Errorf("Failed to open a channel")
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello",
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		c.Errorf("Failed to declare a queue")
		return err
	}

	err = ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		c.Errorf("Failed to publish a message")
		return err
	}

	return nil
}
