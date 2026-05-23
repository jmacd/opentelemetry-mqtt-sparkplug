// Package sparkplugclient provides a thin wrapper around paho.mqtt.golang for
// publishing and subscribing to Sparkplug B messages.
package sparkplugclient

import (
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/jmacd/opentelemetry-mqtt-sparkplug/sparkplug"
	sparkproto "github.com/jmacd/opentelemetry-mqtt-sparkplug/sparkplug/proto"
	"google.golang.org/protobuf/proto"
)

// Client wraps a paho MQTT client with Sparkplug-aware publish/subscribe helpers.
type Client struct {
	client paho.Client
}

// Options wraps paho.ClientOptions so callers do not need to import paho directly.
type Options struct {
	paho.ClientOptions
}

// NewOptions returns a new Options with paho defaults.
func NewOptions() *Options {
	return &Options{ClientOptions: *paho.NewClientOptions()}
}

// NewClient creates a Client from the given Options.
func NewClient(opts *Options) Client {
	return Client{client: paho.NewClient(&opts.ClientOptions)}
}

// Connect connects to the broker and returns the paho Token.
func (c Client) Connect() paho.Token {
	return c.client.Connect()
}

// Disconnect waits up to quiesce milliseconds and then disconnects.
func (c Client) Disconnect(quiesce uint) {
	c.client.Disconnect(quiesce)
}

// PublishSparkplug marshals payload and publishes it on the Sparkplug B topic
// derived from t.  It returns the paho Token.
func (c Client) PublishSparkplug(t sparkplug.Topic, qos byte, retained bool, payload *sparkproto.Payload) paho.Token {
	data, err := proto.Marshal(payload)
	if err != nil {
		// proto.Marshal only fails when the message contains invalid UTF-8
		// in a string field or similar; panic here mirrors caspar.water behaviour.
		panic(err)
	}
	return c.client.Publish(t.String(), qos, retained, data)
}
