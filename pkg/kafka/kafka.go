package kafka

import (
	"context"

	"github.com/Prrromanssss/platform_common/pkg/kafka/consumer"
)

type Consumer interface {
	Consume(ctx context.Context, topicName string, handler consumer.Handler) (err error)
	Close() error
}
