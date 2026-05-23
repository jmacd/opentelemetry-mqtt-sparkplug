package sparkplugreceiver

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jmacd/opentelemetry-mqtt-sparkplug/sparkplug"
	sparkproto "github.com/jmacd/opentelemetry-mqtt-sparkplug/sparkplug/proto"
	"github.com/jmacd/opentelemetry-mqtt-sparkplug/sparkplug/sparkplugclient"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// metricsSink is a simple consumer.Metrics implementation that captures all
// metrics passed to ConsumeMetrics.
type metricsSink struct {
	mu      sync.Mutex
	metrics []pmetric.Metrics
}

func (s *metricsSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *metricsSink) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = append(s.metrics, md)
	return nil
}

func (s *metricsSink) allMetrics() []pmetric.Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]pmetric.Metrics, len(s.metrics))
	copy(out, s.metrics)
	return out
}

// freePort finds an available TCP port on localhost and returns its address.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// nopHost satisfies component.Host for tests.
type nopHost struct{}

func (nopHost) GetExtensions() map[component.ID]component.Component { return nil }

// TestE2ESelfHosted starts a self-hosted receiver, connects a Sparkplug Edge
// Node via paho.mqtt.golang, publishes NBIRTH / DBIRTH / DDATA messages, then
// calls flush() and verifies that the expected metrics are produced.
func TestE2ESelfHosted(t *testing.T) {
	addr := freePort(t)

	sink := &metricsSink{}

	cfg := Config{
		Broker: BrokerConfig{
			AddrConfig: confignet.AddrConfig{
				Endpoint:  addr,
				Transport: "tcp",
			},
			SelfHosted: true,
			HostID:     "test-host",
		},
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	set := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}

	recv, err := New(set, cfg, sink)
	require.NoError(t, err)

	err = recv.Start(context.Background(), nopHost{})
	require.NoError(t, err)
	defer func() {
		_ = recv.Shutdown(context.Background())
	}()

	// Connect the Sparkplug client to the self-hosted broker.
	opts := sparkplugclient.NewOptions()
	opts.AddBroker("tcp://" + addr).
		SetClientID("test-edge-node").
		SetCleanSession(true).
		SetConnectionLostHandler(func(_ pahomqtt.Client, connErr error) {
			t.Logf("connection lost: %v", connErr)
		})

	client := sparkplugclient.NewClient(opts)
	token := client.Connect()
	require.True(t, token.WaitTimeout(5*time.Second))
	require.NoError(t, token.Error())
	defer client.Disconnect(250)

	now := uint64(time.Now().UnixMilli())

	// Publish NBIRTH for the edge node.
	nbirthTopic := sparkplug.NewTopic("testgroup", sparkplug.NBIRTH, "testnode", "")
	nbirthPayload := &sparkproto.Payload{
		Timestamp: &now,
		Seq:       proto64(0),
		Metrics: []*sparkproto.Payload_Metric{
			{
				Name:      strPtr("bdSeq"),
				Timestamp: &now,
				Value:     &sparkproto.Payload_Metric_IntValue{IntValue: 0},
			},
			{
				Name:      strPtr("Node Properties/Description"),
				Timestamp: &now,
				Value:     &sparkproto.Payload_Metric_StringValue{StringValue: "Test Node"},
			},
		},
	}
	token = client.PublishSparkplug(nbirthTopic, 1, false, nbirthPayload)
	require.True(t, token.WaitTimeout(5*time.Second))
	require.NoError(t, token.Error())

	// Publish DBIRTH for a device with a "temperature" metric.
	dbirthTopic := sparkplug.NewTopic("testgroup", sparkplug.DBIRTH, "testnode", "testdevice")
	dbirthPayload := &sparkproto.Payload{
		Timestamp: &now,
		Seq:       proto64(1),
		Metrics: []*sparkproto.Payload_Metric{
			{
				Name:      strPtr("temperature"),
				Timestamp: &now,
				Value:     &sparkproto.Payload_Metric_FloatValue{FloatValue: 25.0},
			},
		},
	}
	token = client.PublishSparkplug(dbirthTopic, 1, false, dbirthPayload)
	require.True(t, token.WaitTimeout(5*time.Second))
	require.NoError(t, token.Error())

	// Publish DDATA with an updated temperature reading.
	ddataTopic := sparkplug.NewTopic("testgroup", sparkplug.DDATA, "testnode", "testdevice")
	later := uint64(time.Now().UnixMilli())
	ddataPayload := &sparkproto.Payload{
		Timestamp: &later,
		Seq:       proto64(2),
		Metrics: []*sparkproto.Payload_Metric{
			{
				Name:      strPtr("temperature"),
				Timestamp: &later,
				Value:     &sparkproto.Payload_Metric_FloatValue{FloatValue: 26.5},
			},
		},
	}
	token = client.PublishSparkplug(ddataTopic, 1, false, ddataPayload)
	require.True(t, token.WaitTimeout(5*time.Second))
	require.NoError(t, token.Error())

	// Give the broker time to deliver all messages to the hook.
	time.Sleep(200 * time.Millisecond)

	// Call flush() directly (same package) to trigger ConsumeMetrics.
	r := recv.(*sparkplugReceiver)
	require.NoError(t, r.flush())

	// Verify that we received at least one batch of metrics.
	all := sink.allMetrics()
	require.NotEmpty(t, all, "expected metrics from flush")

	// Find the ResourceMetrics for our device and check the temperature metric.
	found := false
	for _, md := range all {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			rm := md.ResourceMetrics().At(i)
			attrs := rm.Resource().Attributes()

			gid, ok := attrs.Get("group_id")
			if !ok || gid.Str() != "testgroup" {
				continue
			}
			did, ok := attrs.Get("device_id")
			if !ok || did.Str() != "testdevice" {
				continue
			}

			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					m := sm.Metrics().At(k)
					if m.Name() == "temperature" {
						require.Equal(t, 1, m.Gauge().DataPoints().Len())
						dp := m.Gauge().DataPoints().At(0)
						require.InDelta(t, 26.5, dp.DoubleValue(), 0.01)
						found = true
					}
				}
			}
		}
	}
	require.True(t, found, "temperature metric not found in flushed output")
}

// proto64 returns a pointer to a uint64.
func proto64(v uint64) *uint64 {
	return &v
}

// strPtr returns a pointer to a string.
func strPtr(s string) *string {
	return &s
}
