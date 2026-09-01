package authorization

import (
	"time"

	platformauthz "github.com/lihongjie0209/microservice-platform-go/authz"
	authorizationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/authorization/v1"
	"github.com/lihongjie0209/service-registry-service/internal/outbound"
)

func New(registry *outbound.Registry) platformauthz.Authorizer {
	connection, ok := registry.GRPC("authorization")
	if !ok {
		return platformauthz.NewGRPCAuthorizer(nil, 2*time.Second)
	}
	return platformauthz.NewGRPCAuthorizer(authorizationv1.NewAuthorizationServiceClient(connection), 2*time.Second)
}
