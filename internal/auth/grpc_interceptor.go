package auth

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// GatewayGRPCUnaryInterceptor creates a unary gRPC server interceptor for authentication and rate limiting.
func GatewayGRPCUnaryInterceptor(
	authenticator GatewayAuthenticator,
	rateLimiter RateLimiter,
	audit AuditLogger,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		clientIP := ExtractPeerIP(ctx)

		identity, err := authenticator.AuthenticateGRPC(ctx)
		if err != nil {
			if audit != nil {
				audit.LogAuthFailure(clientIP, "gRPC", err.Error(), 401, int(codes.Unauthenticated))
			}
			return nil, status.Errorf(codes.Unauthenticated, "authentication required: %v", err)
		}

		if rateLimiter != nil {
			allowed, _ := rateLimiter.Allow(identity.DeviceID)
			if !allowed {
				if audit != nil {
					audit.LogRateLimited(clientIP, identity.DeviceID)
				}
				return nil, status.Errorf(codes.ResourceExhausted, "device %q rate limit exceeded", identity.DeviceID)
			}
		}

		ctx = WithDeviceIdentity(ctx, identity)
		return handler(ctx, req)
	}
}

// wrappedServerStream wraps grpc.ServerStream with a modified context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// GatewayGRPCStreamInterceptor creates a stream gRPC server interceptor for authentication and rate limiting.
func GatewayGRPCStreamInterceptor(
	authenticator GatewayAuthenticator,
	rateLimiter RateLimiter,
	audit AuditLogger,
) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx := ss.Context()
		clientIP := ExtractPeerIP(ctx)

		identity, err := authenticator.AuthenticateGRPC(ctx)
		if err != nil {
			if audit != nil {
				audit.LogAuthFailure(clientIP, "gRPC", err.Error(), 401, int(codes.Unauthenticated))
			}
			return status.Errorf(codes.Unauthenticated, "authentication required: %v", err)
		}

		if rateLimiter != nil {
			allowed, _ := rateLimiter.Allow(identity.DeviceID)
			if !allowed {
				if audit != nil {
					audit.LogRateLimited(clientIP, identity.DeviceID)
				}
				return status.Errorf(codes.ResourceExhausted, "device %q rate limit exceeded", identity.DeviceID)
			}
		}

		ctx = WithDeviceIdentity(ctx, identity)
		return handler(srv, &wrappedServerStream{
			ServerStream: ss,
			ctx:          ctx,
		})
	}
}

func ExtractPeerIP(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "unknown"
}
