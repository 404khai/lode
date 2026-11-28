package vein

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// IdentityHeader carries the vein ID a request is meant for. A vein refuses
// requests addressed to another vein: after container or address churn a
// stale connection can land on the wrong node, and a read must not answer
// (or a write be recorded) for a vein that did not handle it.
const IdentityHeader = "lode-vein-id"

// WrongVeinMarker appears in the error a vein returns for a misaddressed request.
const WrongVeinMarker = "lode: wrong vein"

func checkIdentity(ctx context.Context, self string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	if want := md.Get(IdentityHeader); len(want) > 0 && want[0] != self {
		return status.Errorf(codes.Unavailable, "%s: request for %s reached %s", WrongVeinMarker, want[0], self)
	}
	return nil
}

// IdentityInterceptors return server interceptors enforcing IdentityHeader.
func IdentityInterceptors(self string) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			if err := checkIdentity(ctx, self); err != nil {
				return nil, err
			}
			return h(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			if err := checkIdentity(ss.Context(), self); err != nil {
				return err
			}
			return h(srv, ss)
		}),
	}
}

// IsWrongVein reports whether err is a misaddressed-request refusal.
func IsWrongVein(err error) bool { return err != nil && strings.Contains(err.Error(), WrongVeinMarker) }
