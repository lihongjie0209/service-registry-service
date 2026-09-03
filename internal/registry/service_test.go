package registry

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"github.com/redis/go-redis/v9"
)

func TestLeaseLifecycleAndOwnership(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	store := NewStore(redis.NewClient(&redis.Options{Addr: server.Addr()}))
	service := NewService(store)
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	service.now = func() time.Time { return now }

	registered, err := service.Register(context.Background(), &registryv1.RegisterInstanceRequest{Instance: &registryv1.ServiceInstance{ServiceName: "tenant-service", InstanceId: "pod-1", Endpoint: "grpc://tenant-service:9090", Metadata: map[string]string{"dictionary-provider": "true"}}, LeaseSeconds: 10})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if registered.GetLeaseToken() == "" || registered.GetRevision() != 1 {
		t.Fatalf("unexpected registration: %+v", registered)
	}
	if _, err := service.Register(context.Background(), &registryv1.RegisterInstanceRequest{Instance: registered.Instance, LeaseSeconds: 10}); err != ErrAlreadyRegistered {
		t.Fatalf("duplicate error = %v", err)
	}

	list, err := service.List(context.Background(), &registryv1.ListInstancesRequest{ServiceName: "tenant-service", Selector: &registryv1.MetadataSelector{Match: map[string]string{"dictionary-provider": "true"}}})
	if err != nil || len(list.Instances) != 1 {
		t.Fatalf("list = %+v, %v", list, err)
	}
	if _, err := service.Renew(context.Background(), &registryv1.RenewLeaseRequest{ServiceName: "tenant-service", InstanceId: "pod-1", LeaseToken: "wrong", LeaseSeconds: 10}); err != ErrLeaseNotOwned {
		t.Fatalf("wrong token error = %v", err)
	}

	renewed, err := service.Renew(context.Background(), &registryv1.RenewLeaseRequest{ServiceName: "tenant-service", InstanceId: "pod-1", LeaseToken: registered.LeaseToken, LeaseSeconds: 10})
	if err != nil || renewed.Revision != 2 {
		t.Fatalf("renew = %+v, %v", renewed, err)
	}
	changes, err := store.Changes(context.Background(), 0, time.Millisecond)
	if err != nil || len(changes) != 2 || changes[1].Revision != 2 {
		t.Fatalf("changes = %+v, %v", changes, err)
	}
	if _, err := service.Deregister(context.Background(), &registryv1.DeregisterInstanceRequest{ServiceName: "tenant-service", InstanceId: "pod-1", LeaseToken: registered.LeaseToken}); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	if _, err := store.Get(context.Background(), "tenant-service", "pod-1"); err != ErrNotFound {
		t.Fatalf("get after deregister = %v", err)
	}
}

func TestListFiltersDrainingAndMetadata(t *testing.T) {
	t.Parallel()
	selector := &registryv1.MetadataSelector{Match: map[string]string{"kind": "dictionary"}}
	if !matches(&registryv1.ServiceInstance{Status: registryv1.InstanceStatus_INSTANCE_STATUS_HEALTHY, Metadata: map[string]string{"kind": "dictionary"}}, selector, false) {
		t.Fatal("healthy matching instance was filtered")
	}
	if matches(&registryv1.ServiceInstance{Status: registryv1.InstanceStatus_INSTANCE_STATUS_DRAINING, Metadata: map[string]string{"kind": "dictionary"}}, selector, false) {
		t.Fatal("draining instance was included")
	}
	if !matches(&registryv1.ServiceInstance{Status: registryv1.InstanceStatus_INSTANCE_STATUS_DRAINING, Metadata: map[string]string{"kind": "dictionary"}}, selector, true) {
		t.Fatal("requested draining instance was filtered")
	}
}

func TestValidateInstanceRejectsUnsafeEndpoint(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []string{"", "file:///etc/passwd", "tenant-service:9090"} {
		if err := validateInstance(&registryv1.ServiceInstance{ServiceName: "tenant-service", InstanceId: "pod-1", Endpoint: endpoint}); err == nil {
			t.Fatalf("endpoint %q accepted", endpoint)
		}
	}
}

func TestPageValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		page     int
		pageSize int
		want     []int
	}{
		{name: "unpaged compatibility", want: []int{1, 2, 3, 4, 5}},
		{name: "middle page", page: 2, pageSize: 2, want: []int{3, 4}},
		{name: "partial last page", page: 3, pageSize: 2, want: []int{5}},
		{name: "past last page", page: 4, pageSize: 2, want: []int{}},
		{name: "overflow safe", page: int(^uint(0) >> 1), pageSize: 100, want: []int{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := pageValues([]int{1, 2, 3, 4, 5}, test.page, test.pageSize)
			if len(got) != len(test.want) {
				t.Fatalf("pageValues() = %v, want %v", got, test.want)
			}
			for index := range got {
				if got[index] != test.want[index] {
					t.Fatalf("pageValues() = %v, want %v", got, test.want)
				}
			}
		})
	}
}
