package sparkplugreceiver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/jmacd/opentelemetry-mqtt-sparkplug/otlp"
	"github.com/jmacd/opentelemetry-mqtt-sparkplug/sparkplug"
	sparkproto "github.com/jmacd/opentelemetry-mqtt-sparkplug/sparkplug/proto"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	nodeControlPrefix    = "Node Control/"
	nodePropertiesPrefix = "Node Properties/"

	deviceControlPrefix    = "Device Control/"
	devicePropertiesPrefix = "Device Properties/"

	libraryName = "OptoMMP/Modules/Channels"
)

var (
	// Hacky denylist
	denyNames = map[string]bool{
		"Device Properties/Tag Access Time Ms": true,
		"Device Properties/Write Queue Depth":  true,
		"Device Properties/Max Scan Time Ms":   true,
	}
)

type sparkplugReceiver struct {
	lock         sync.Mutex
	settings     receiver.Settings
	config       Config
	allowMetrics map[string]bool
	nextConsumer consumer.Metrics
	broker       *mqtt.Server
	brokerDone   chan error
	state        otlp.SparkplugState
	lastUpdate   time.Time
}

var (
	// ErrUnexpectedTopic happens in self-hosted mode where we
	// do not expect another broker or another host application
	// to be using MQTT.
	ErrUnexpectedTopic = fmt.Errorf("unexpected topic")
)

// New creates the Sparkplug receiver with the given parameters.
func New(
	set receiver.Settings,
	config Config,
	nextConsumer consumer.Metrics,
) (receiver.Metrics, error) {
	if nextConsumer == nil {
		return nil, fmt.Errorf("invalid next consumer: nil")
	}

	if config.Broker.AddrConfig.Endpoint == "" {
		config.Broker.AddrConfig.Transport = "tcp"
		config.Broker.AddrConfig.Endpoint = "localhost:1883"
	}

	r := &sparkplugReceiver{
		settings:     set,
		config:       config,
		nextConsumer: nextConsumer,
		state:        otlp.SparkplugState{}.Init(),
	}
	if len(config.Metrics) != 0 {
		r.allowMetrics = map[string]bool{}
		for _, allow := range config.Metrics {
			r.allowMetrics[allow] = true
		}
	}

	return r, nil
}

func (r *sparkplugReceiver) Start(ctx context.Context, host component.Host) error {
	if !r.config.Broker.SelfHosted {
		return fmt.Errorf("not implemented: passive client mode")
	}

	if err := r.startBroker(ctx); err != nil {
		return err
	}
	r.settings.Logger.Info(
		"self-hosted broker start",
		zap.String("host_id", r.config.Broker.HostID),
		zap.String("endpoint", r.config.Broker.AddrConfig.Endpoint),
	)
	return nil
}

func (r *sparkplugReceiver) Shutdown(context.Context) error {
	_ = r.broker.Close()
	return <-r.brokerDone
}

// sparkplugHook implements mqtt.Hook to handle broker events.
type sparkplugHook struct {
	mqtt.HookBase
	receiver *sparkplugReceiver
}

func (h *sparkplugHook) ID() string {
	return "sparkplug-receiver"
}

func (h *sparkplugHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnect,
		mqtt.OnDisconnect,
		mqtt.OnPublish,
	}, []byte{b})
}

func (h *sparkplugHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	h.receiver.settings.Logger.Info(
		"client connected",
		zap.String("client_id", cl.ID),
		zap.String("remote_addr", cl.Net.Remote),
	)
	return nil
}

func (h *sparkplugHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	h.receiver.settings.Logger.Warn(
		"client disconnected",
		zap.String("client_id", cl.ID),
		zap.String("remote_addr", cl.Net.Remote),
		zap.Error(err),
	)
}

func (h *sparkplugHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	pk, err := h.receiver.onMessage(cl, pk)
	if err != nil {
		h.receiver.settings.Logger.Warn(
			"message error",
			zap.String("client_id", cl.ID),
			zap.String("remote_addr", cl.Net.Remote),
			zap.Error(err),
		)
	}
	return pk, err
}

func (r *sparkplugReceiver) startBroker(context.Context) error {
	r.broker = mqtt.New(nil)
	r.brokerDone = make(chan error)

	switch r.config.Broker.AddrConfig.Transport {
	case "tcp":
		break
	default:
		return fmt.Errorf("transport unsupported: %v",
			r.config.Broker.AddrConfig.Transport)
	}

	// Allow all connections.
	if err := r.broker.AddHook(new(auth.AllowHook), nil); err != nil {
		return err
	}

	// Add custom hook for Sparkplug event handling.
	if err := r.broker.AddHook(&sparkplugHook{receiver: r}, nil); err != nil {
		return err
	}

	tcp := listeners.NewTCP(listeners.Config{
		ID:      "sparkplug-tcp",
		Address: r.config.Broker.AddrConfig.Endpoint,
	})
	if err := r.broker.AddListener(tcp); err != nil {
		return err
	}

	// Publish the application state with retain=true.
	if err := r.broker.Publish("STATE/"+r.config.Broker.HostID, []byte("ONLINE"), true, 0); err != nil {
		return err
	}

	go func() {
		r.settings.Logger.Info(
			"listening",
			zap.String("endpoint", r.config.Broker.AddrConfig.Endpoint),
		)
		r.brokerDone <- r.broker.Serve()
	}()

	go func() {
		for {
			time.Sleep(time.Second * 30)
			if err := r.flush(); err != nil {
				panic("error in flush")
			}
		}
	}()

	return nil
}

func (r *sparkplugReceiver) onMessage(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if !strings.HasPrefix(pk.TopicName, sparkplug.BTopicPrefix) {
		// A "STATE/host_id" message is valid but
		// unexpected.  In a self-hosted broker it's not
		// clear who would do this or why.
		return pk, fmt.Errorf("%w: %s", ErrUnexpectedTopic, pk.TopicName)
	}

	topic, err := sparkplug.ParseTopic(pk.TopicName)
	if err != nil {
		return pk, fmt.Errorf("parse topic: %w: %s", err, pk.TopicName)
	}

	b := &sparkproto.Payload{}
	if err := proto.Unmarshal(pk.Payload, b); err != nil {
		return pk, fmt.Errorf("payload unmarshal: %v: %w", pk.TopicName, err)
	}

	return pk, r.sparkplugPayload(topic, b)
}

func (r *sparkplugReceiver) sparkplugPayload(topic sparkplug.Topic, payload *sparkproto.Payload) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.lastUpdate = time.Now()

	switch topic.MessageType {
	case sparkplug.NDATA, sparkplug.NBIRTH, sparkplug.NDEATH:
		return r.sparkplugNodePayload(topic, payload)
	case sparkplug.DDATA, sparkplug.DBIRTH, sparkplug.DDEATH:
		return r.sparkplugDevicePayload(topic, payload)
	}
	// Unexpected in a self-hosted broker situation.  STATE/* was
	// checked above, so these are node/device commands.
	return fmt.Errorf("%w: %v", ErrUnexpectedTopic, topic.MessageType)
}

func (r *sparkplugReceiver) sparkplugNodePayload(topic sparkplug.Topic, payload *sparkproto.Payload) error {
	node := r.state.Get(topic.GroupID).Get(topic.EdgeNodeID)
	return node.Visit(topic, payload)
}

func (r *sparkplugReceiver) nodeToResource(groupID sparkplug.GroupID, edgeNodeID sparkplug.EdgeNodeID, node otlp.EdgeNodeState) pmetric.Metrics {
	m := pmetric.NewMetrics()
	rm := m.ResourceMetrics().AppendEmpty()

	rm.Resource().Attributes().PutStr(
		"group_id",
		string(groupID),
	)
	rm.Resource().Attributes().PutStr(
		"edgenode_id",
		string(edgeNodeID),
	)

	for _, metric := range node.Store.NameMap {
		switch {

		case metric.Name == "bdSeq":
			continue

		case strings.HasPrefix(metric.Name, nodeControlPrefix):
			continue

		case strings.HasPrefix(metric.Name, nodePropertiesPrefix):
			anyValue(metric.Value).CopyTo(
				rm.Resource().Attributes().PutEmpty(
					resourceName(metric.Name[len(nodePropertiesPrefix):]),
				),
			)
			continue
		}

		r.settings.Logger.Warn(
			"unexpected edge node metric",
			zap.String("name", metric.Name),
		)
	}
	return m
}

func resourceName(name string) string {
	return strings.Replace(strings.ToLower(name), " ", "_", -1)
}

func metricName(name string) string {
	return strings.Replace(strings.ToLower(name), "/", "_", -1)
}

func anyValue(value interface{}) pcommon.Value {
	switch t := value.(type) {
	case *sparkproto.Payload_Metric_IntValue:
		return pcommon.NewValueInt(int64(t.IntValue))
	case *sparkproto.Payload_Metric_LongValue:
		return pcommon.NewValueInt(int64(t.LongValue))
	case *sparkproto.Payload_Metric_FloatValue:
		return pcommon.NewValueDouble(float64(t.FloatValue))
	case *sparkproto.Payload_Metric_DoubleValue:
		return pcommon.NewValueDouble(t.DoubleValue)
	case *sparkproto.Payload_Metric_BooleanValue:
		return pcommon.NewValueBool(t.BooleanValue)
	case *sparkproto.Payload_Metric_StringValue:
		return pcommon.NewValueStr(t.StringValue)
	case *sparkproto.Payload_Metric_BytesValue:
		return pcommon.NewValueStr(string(t.BytesValue))

	case *sparkproto.Payload_Metric_DatasetValue,
		*sparkproto.Payload_Metric_TemplateValue,
		*sparkproto.Payload_Metric_ExtensionValue:
		break
	}
	return pcommon.NewValueStr(fmt.Sprintf("unsupported attribute type: %T", value))
}

func (r *sparkplugReceiver) setNumberValue(point pmetric.NumberDataPoint, value interface{}) {
	switch t := value.(type) {
	case *sparkproto.Payload_Metric_IntValue:
		point.SetIntValue(int64(t.IntValue))
	case *sparkproto.Payload_Metric_LongValue:
		point.SetIntValue(int64(t.LongValue))
	case *sparkproto.Payload_Metric_FloatValue:
		point.SetDoubleValue(float64(t.FloatValue))
	case *sparkproto.Payload_Metric_DoubleValue:
		point.SetDoubleValue(t.DoubleValue)
	default:
		point.SetDoubleValue(math.NaN())
	}
}

func (r *sparkplugReceiver) sparkplugDevicePayload(topic sparkplug.Topic, payload *sparkproto.Payload) error {
	node := r.state.Get(topic.GroupID).Get(topic.EdgeNodeID)
	device := node.Get(topic.DeviceID)
	return device.Visit(topic, payload)
}

func (r *sparkplugReceiver) flush() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	now := time.Now()
	nowTS := pcommon.NewTimestampFromTime(now)

	for groupID, groupState := range r.state.Items {
		for edgeNodeID, edgeNode := range groupState.Items {
			for deviceID, deviceNode := range edgeNode.Items {
				metrics := r.nodeToResource(groupID, edgeNodeID, edgeNode)
				rm := metrics.ResourceMetrics().At(0)

				ilm := rm.ScopeMetrics().AppendEmpty()

				rm.Resource().Attributes().PutStr(
					"device_id",
					string(deviceID),
				)

				// Hacky hard-coded library name
				ilm.Scope().SetName(libraryName)

				metric := ilm.Metrics().AppendEmpty()
				metric.SetName("staleness")
				metric.SetUnit("s")
				metric.SetEmptyGauge()
				dp := metric.Gauge().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowTS)
				dp.SetDoubleValue(now.Sub(*deviceNode.LastTime).Seconds())

				for _, metric := range deviceNode.Store.NameMap {

					if denyNames[metric.Name] {
						continue
					}

					if strings.HasPrefix(metric.Name, deviceControlPrefix) {
						continue
					}

					if strings.HasPrefix(metric.Name, devicePropertiesPrefix) {
						anyValue(metric.Value).CopyTo(
							rm.Resource().Attributes().PutEmpty(
								resourceName(metric.Name[len(devicePropertiesPrefix):]),
							),
						)
						continue
					}

					name := metric.Name
					if strings.HasPrefix(name, libraryName) {
						name = name[len(libraryName)+1:]
					}
					name = metricName(name)
					if r.allowMetrics != nil && !r.allowMetrics[name] {
						continue
					}

					output := ilm.Metrics().AppendEmpty()
					output.SetName(name)
					output.SetEmptyGauge()

					dp := output.Gauge().DataPoints().AppendEmpty()
					dp.SetTimestamp(nowTS)
					dp.SetStartTimestamp(pcommon.Timestamp(deviceNode.BirthTime.UnixNano()))

					r.setNumberValue(dp, metric.Value)
				}

				if err := r.nextConsumer.ConsumeMetrics(context.Background(), metrics); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
