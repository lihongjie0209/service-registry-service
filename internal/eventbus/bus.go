package eventbus

import (
	"context"
	"errors"

	platformeventbus "github.com/lihongjie0209/microservice-platform-go/eventbus"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"github.com/lihongjie0209/service-registry-service/internal/config"
)

type Bus = platformeventbus.Bus
type Handler = platformeventbus.Handler
type Envelope = commonv1.EventEnvelope

func New(ctx context.Context, cfg config.Config) (*Bus, error) {
	if !cfg.EventBus.Enabled {
		return nil, nil
	}
	eventCfg := cfg.EventBus
	return platformeventbus.New(ctx, platformeventbus.Config{
		URLs:               eventCfg.URLs,
		ClientName:         cfg.App.Name,
		StreamName:         eventCfg.StreamName,
		Subjects:           eventCfg.Subjects,
		Storage:            eventCfg.Storage,
		MaxAge:             eventCfg.MaxAge,
		DuplicateWindow:    eventCfg.DuplicateWindow,
		ConnectTimeout:     eventCfg.ConnectTimeout,
		ReconnectWait:      eventCfg.ReconnectWait,
		PublishTimeout:     eventCfg.PublishTimeout,
		ConsumerAckWait:    eventCfg.ConsumerAckWait,
		ConsumerMaxDeliver: eventCfg.ConsumerMaxDeliver,
	})
}

func Publish(ctx context.Context, bus *Bus, subject string, envelope *Envelope) error {
	if bus == nil {
		return errors.New("event bus is disabled")
	}
	return bus.Publish(ctx, subject, envelope)
}

func Close(bus *Bus) error {
	if bus == nil {
		return nil
	}
	return bus.Close()
}
