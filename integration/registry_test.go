//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"github.com/lihongjie0209/service-registry-service/internal/registry"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestRegistryLeaseExpiryAndResumeStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute); defer cancel()
	container, err := rediscontainer.Run(ctx, "redis:7.4-alpine"); if err != nil { t.Fatal(err) }; testcontainers.CleanupContainer(t, container)
	connectionString, err := container.ConnectionString(ctx); if err != nil { t.Fatal(err) }; options, err := goredis.ParseURL(connectionString); if err != nil { t.Fatal(err) }; client := goredis.NewClient(options); t.Cleanup(func(){_ = client.Close()})
	store := registry.NewStore(client); service := registry.NewService(store)
	registered, err := service.Register(ctx,&registryv1.RegisterInstanceRequest{Instance:&registryv1.ServiceInstance{ServiceName:"orders-service",InstanceId:"pod-a",Endpoint:"grpc://orders-service:9090",Metadata:map[string]string{"role":"provider"}},LeaseSeconds:5}); if err != nil { t.Fatal(err) }
	if registered.Revision != 1 { t.Fatalf("revision=%d",registered.Revision) }
	changes, err := store.Changes(ctx,0,time.Second); if err != nil || len(changes)!=1 { t.Fatalf("changes=%+v err=%v",changes,err) }
	if err := client.Del(ctx,"platform:registry:instance:orders-service:pod-a","platform:registry:token:orders-service:pod-a").Err(); err != nil { t.Fatal(err) }
	listed, err := service.List(ctx,&registryv1.ListInstancesRequest{ServiceName:"orders-service"}); if err != nil { t.Fatal(err) }; if len(listed.Instances)!=0 { t.Fatalf("expired instance remained: %+v",listed.Instances) }
}
