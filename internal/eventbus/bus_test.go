package eventbus

import (
	"context"
	"testing"
)

func TestDisabledBus(t *testing.T) {
	t.Parallel()
	if err := Publish(context.Background(), nil, "platform.example.created.v1", &Envelope{}); err == nil {
		t.Fatal("Publish() error = nil")
	}
	if err := Close(nil); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
