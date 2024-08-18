package consumer

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2/log"
)

type Handler func(ctx context.Context, msg *sarama.ConsumerMessage) error

type GroupHandler struct {
	msgHandler Handler
}

func NewGroupHandler() *GroupHandler {
	return &GroupHandler{}
}

// Setup is called at the beginning of a new session before ConsumeClaim is called.
func (c *GroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is called at the end of the session after all ConsumeClaim goroutines have completed.
func (c *GroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim should start the consumer loop for the ConsumerGroupClaim().
// After the Messages() channel is closed, the handler should complete its processing.
func (c *GroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// The code below should not be moved to a goroutine, as ConsumeClaim
	// is already running in a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L869
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Infof("message channel was closed\n")
				return nil
			}

			log.Infof("message claimed: value = %s, timestamp = %v, topic = %s\n", string(message.Value), message.Timestamp, message.Topic)

			err := c.msgHandler(session.Context(), message)
			if err != nil {
				log.Infof("error handling message: %v\n", err)
				continue
			}

			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// Otherwise, `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` may occur during Kafka rebalance. See:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			log.Infof("session context done\n")
			return nil
		}
	}
}
