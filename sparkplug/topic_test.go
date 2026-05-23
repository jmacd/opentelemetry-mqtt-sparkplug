package sparkplug

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testTopics = []struct {
	Expect string
	Topic  Topic
}{
	{
		"spBv1.0/grp/NBIRTH/node",
		Topic{
			GroupID:     "grp",
			MessageType: NBIRTH,
			EdgeNodeID:  "node",
		},
	},
	{
		"spBv1.0/grp/DDEATH/node/dev",
		Topic{
			GroupID:     "grp",
			MessageType: DDEATH,
			EdgeNodeID:  "node",
			DeviceID:    "dev",
		},
	},
}

func TestTopicString(t *testing.T) {
	for _, test := range testTopics {
		require.Equal(t, test.Expect, test.Topic.String())
	}
}

func TestTopicStringWildcardTruncatesSuffix(t *testing.T) {
	topic := Topic{
		GroupID:     "#",
		MessageType: DBIRTH,
		EdgeNodeID:  "node",
		DeviceID:    "device",
	}
	require.Equal(t, "spBv1.0/#", topic.String())
}

func TestTopicParse(t *testing.T) {
	for _, test := range testTopics {
		topic, err := ParseTopic(test.Expect)
		require.NoError(t, err)
		require.Equal(t, test.Topic, topic)
	}
}

func TestTopicError(t *testing.T) {
	parseError := func(s string) error {
		_, err := ParseTopic(s)
		return err
	}
	require.Error(t, parseError("a/b/c/d"))
	require.Error(t, parseError("spBv1.0//c/d"))
	require.Error(t, parseError("spBv1.0/b/c/d"))
	require.Error(t, parseError("spBv1.0/b/NDEATH/d/e/f"))
}

func TestMessageTypeIsBirth(t *testing.T) {
	require.True(t, NBIRTH.IsBirth())
	require.True(t, DBIRTH.IsBirth())
	require.False(t, NDEATH.IsBirth())
}

func TestNewTopic(t *testing.T) {
	require.Equal(t, Topic{
		GroupID:     "grp",
		MessageType: NCMD,
		EdgeNodeID:  "node",
		DeviceID:    "dev",
	}, NewTopic("grp", NCMD, "node", "dev"))
}
