package storage

import (
	"context"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// serverClient adapts the in-process etcd server's pb.*Server handlers (with
// their header-fill and request validation, via v3rpc.New*Server) to the
// pb.*Client interfaces clientv3 expects. Every method drops the unused
// grpc.CallOption args and delegates to the embedded server; the two streaming
// RPCs (Watch, LeaseKeepAlive) splice an in-memory pipe — see spawnStream. No
// grpc transport, no serialization, no loopback TCP.
type serverClient struct {
	pb.KVServer
	pb.WatchServer
	pb.LeaseServer
	pb.MaintenanceServer
	pb.ClusterServer
	pb.AuthServer
}

var (
	_ pb.KVClient          = serverClient{}
	_ pb.WatchClient       = serverClient{}
	_ pb.LeaseClient       = serverClient{}
	_ pb.MaintenanceClient = serverClient{}
	_ pb.ClusterClient     = serverClient{}
	_ pb.AuthClient        = serverClient{}
)

// noopCloseWatcher / noopCloseLease keep the shared singleton's Watcher and Lease
// alive when the factory Close()s a per-resource handle. Every other method
// promotes to the wrapped value; only Close is intercepted.
type noopCloseWatcher struct{ clientv3.Watcher }

func (noopCloseWatcher) Close() error { return nil }

type noopCloseLease struct{ clientv3.Lease }

func (noopCloseLease) Close() error { return nil }

// --- KV ---

func (c serverClient) Range(ctx context.Context, in *pb.RangeRequest, _ ...grpc.CallOption) (*pb.RangeResponse, error) {
	return c.KVServer.Range(ctx, in)
}

func (c serverClient) Put(ctx context.Context, in *pb.PutRequest, _ ...grpc.CallOption) (*pb.PutResponse, error) {
	return c.KVServer.Put(ctx, in)
}

func (c serverClient) DeleteRange(ctx context.Context, in *pb.DeleteRangeRequest, _ ...grpc.CallOption) (*pb.DeleteRangeResponse, error) {
	return c.KVServer.DeleteRange(ctx, in)
}

func (c serverClient) Txn(ctx context.Context, in *pb.TxnRequest, _ ...grpc.CallOption) (*pb.TxnResponse, error) {
	return c.KVServer.Txn(ctx, in)
}

func (c serverClient) Compact(ctx context.Context, in *pb.CompactionRequest, _ ...grpc.CallOption) (*pb.CompactionResponse, error) {
	return c.KVServer.Compact(ctx, in)
}

// --- Watch (bidi stream) ---

func (c serverClient) Watch(ctx context.Context, _ ...grpc.CallOption) (pb.Watch_WatchClient, error) {
	return spawnStream(ctx, func(s bidiServer[pb.WatchRequest, pb.WatchResponse]) error {
		return c.WatchServer.Watch(s)
	}), nil
}

// --- Lease ---

func (c serverClient) LeaseGrant(ctx context.Context, in *pb.LeaseGrantRequest, _ ...grpc.CallOption) (*pb.LeaseGrantResponse, error) {
	return c.LeaseServer.LeaseGrant(ctx, in)
}

func (c serverClient) LeaseRevoke(ctx context.Context, in *pb.LeaseRevokeRequest, _ ...grpc.CallOption) (*pb.LeaseRevokeResponse, error) {
	return c.LeaseServer.LeaseRevoke(ctx, in)
}

func (c serverClient) LeaseTimeToLive(ctx context.Context, in *pb.LeaseTimeToLiveRequest, _ ...grpc.CallOption) (*pb.LeaseTimeToLiveResponse, error) {
	return c.LeaseServer.LeaseTimeToLive(ctx, in)
}

func (c serverClient) LeaseLeases(ctx context.Context, in *pb.LeaseLeasesRequest, _ ...grpc.CallOption) (*pb.LeaseLeasesResponse, error) {
	return c.LeaseServer.LeaseLeases(ctx, in)
}

func (c serverClient) LeaseKeepAlive(ctx context.Context, _ ...grpc.CallOption) (pb.Lease_LeaseKeepAliveClient, error) {
	return spawnStream(ctx, func(s bidiServer[pb.LeaseKeepAliveRequest, pb.LeaseKeepAliveResponse]) error {
		return c.LeaseServer.LeaseKeepAlive(s)
	}), nil
}

// --- Maintenance (Snapshot is server-stream; apiserver never calls it) ---

func (c serverClient) Alarm(ctx context.Context, in *pb.AlarmRequest, _ ...grpc.CallOption) (*pb.AlarmResponse, error) {
	return c.MaintenanceServer.Alarm(ctx, in)
}

func (c serverClient) Status(ctx context.Context, in *pb.StatusRequest, _ ...grpc.CallOption) (*pb.StatusResponse, error) {
	return c.MaintenanceServer.Status(ctx, in)
}

func (c serverClient) Defragment(ctx context.Context, in *pb.DefragmentRequest, _ ...grpc.CallOption) (*pb.DefragmentResponse, error) {
	return c.MaintenanceServer.Defragment(ctx, in)
}

func (c serverClient) Hash(ctx context.Context, in *pb.HashRequest, _ ...grpc.CallOption) (*pb.HashResponse, error) {
	return c.MaintenanceServer.Hash(ctx, in)
}

func (c serverClient) HashKV(ctx context.Context, in *pb.HashKVRequest, _ ...grpc.CallOption) (*pb.HashKVResponse, error) {
	return c.MaintenanceServer.HashKV(ctx, in)
}

func (c serverClient) MoveLeader(ctx context.Context, in *pb.MoveLeaderRequest, _ ...grpc.CallOption) (*pb.MoveLeaderResponse, error) {
	return c.MaintenanceServer.MoveLeader(ctx, in)
}

func (c serverClient) Downgrade(ctx context.Context, in *pb.DowngradeRequest, _ ...grpc.CallOption) (*pb.DowngradeResponse, error) {
	return c.MaintenanceServer.Downgrade(ctx, in)
}

func (c serverClient) Snapshot(ctx context.Context, in *pb.SnapshotRequest, _ ...grpc.CallOption) (pb.Maintenance_SnapshotClient, error) {
	return nil, fmt.Errorf("nanokube: in-process etcd snapshot streaming not supported")
}

// --- Cluster ---

func (c serverClient) MemberAdd(ctx context.Context, in *pb.MemberAddRequest, _ ...grpc.CallOption) (*pb.MemberAddResponse, error) {
	return c.ClusterServer.MemberAdd(ctx, in)
}

func (c serverClient) MemberRemove(ctx context.Context, in *pb.MemberRemoveRequest, _ ...grpc.CallOption) (*pb.MemberRemoveResponse, error) {
	return c.ClusterServer.MemberRemove(ctx, in)
}

func (c serverClient) MemberUpdate(ctx context.Context, in *pb.MemberUpdateRequest, _ ...grpc.CallOption) (*pb.MemberUpdateResponse, error) {
	return c.ClusterServer.MemberUpdate(ctx, in)
}

func (c serverClient) MemberList(ctx context.Context, in *pb.MemberListRequest, _ ...grpc.CallOption) (*pb.MemberListResponse, error) {
	return c.ClusterServer.MemberList(ctx, in)
}

func (c serverClient) MemberPromote(ctx context.Context, in *pb.MemberPromoteRequest, _ ...grpc.CallOption) (*pb.MemberPromoteResponse, error) {
	return c.ClusterServer.MemberPromote(ctx, in)
}

// --- Auth (apiserver never enables auth; present only to keep the client whole) ---

func (c serverClient) AuthEnable(ctx context.Context, in *pb.AuthEnableRequest, _ ...grpc.CallOption) (*pb.AuthEnableResponse, error) {
	return c.AuthServer.AuthEnable(ctx, in)
}

func (c serverClient) AuthDisable(ctx context.Context, in *pb.AuthDisableRequest, _ ...grpc.CallOption) (*pb.AuthDisableResponse, error) {
	return c.AuthServer.AuthDisable(ctx, in)
}

func (c serverClient) AuthStatus(ctx context.Context, in *pb.AuthStatusRequest, _ ...grpc.CallOption) (*pb.AuthStatusResponse, error) {
	return c.AuthServer.AuthStatus(ctx, in)
}

func (c serverClient) Authenticate(ctx context.Context, in *pb.AuthenticateRequest, _ ...grpc.CallOption) (*pb.AuthenticateResponse, error) {
	return c.AuthServer.Authenticate(ctx, in)
}

func (c serverClient) UserAdd(ctx context.Context, in *pb.AuthUserAddRequest, _ ...grpc.CallOption) (*pb.AuthUserAddResponse, error) {
	return c.AuthServer.UserAdd(ctx, in)
}

func (c serverClient) UserGet(ctx context.Context, in *pb.AuthUserGetRequest, _ ...grpc.CallOption) (*pb.AuthUserGetResponse, error) {
	return c.AuthServer.UserGet(ctx, in)
}

func (c serverClient) UserList(ctx context.Context, in *pb.AuthUserListRequest, _ ...grpc.CallOption) (*pb.AuthUserListResponse, error) {
	return c.AuthServer.UserList(ctx, in)
}

func (c serverClient) UserDelete(ctx context.Context, in *pb.AuthUserDeleteRequest, _ ...grpc.CallOption) (*pb.AuthUserDeleteResponse, error) {
	return c.AuthServer.UserDelete(ctx, in)
}

func (c serverClient) UserChangePassword(ctx context.Context, in *pb.AuthUserChangePasswordRequest, _ ...grpc.CallOption) (*pb.AuthUserChangePasswordResponse, error) {
	return c.AuthServer.UserChangePassword(ctx, in)
}

func (c serverClient) UserGrantRole(ctx context.Context, in *pb.AuthUserGrantRoleRequest, _ ...grpc.CallOption) (*pb.AuthUserGrantRoleResponse, error) {
	return c.AuthServer.UserGrantRole(ctx, in)
}

func (c serverClient) UserRevokeRole(ctx context.Context, in *pb.AuthUserRevokeRoleRequest, _ ...grpc.CallOption) (*pb.AuthUserRevokeRoleResponse, error) {
	return c.AuthServer.UserRevokeRole(ctx, in)
}

func (c serverClient) RoleAdd(ctx context.Context, in *pb.AuthRoleAddRequest, _ ...grpc.CallOption) (*pb.AuthRoleAddResponse, error) {
	return c.AuthServer.RoleAdd(ctx, in)
}

func (c serverClient) RoleGet(ctx context.Context, in *pb.AuthRoleGetRequest, _ ...grpc.CallOption) (*pb.AuthRoleGetResponse, error) {
	return c.AuthServer.RoleGet(ctx, in)
}

func (c serverClient) RoleList(ctx context.Context, in *pb.AuthRoleListRequest, _ ...grpc.CallOption) (*pb.AuthRoleListResponse, error) {
	return c.AuthServer.RoleList(ctx, in)
}

func (c serverClient) RoleDelete(ctx context.Context, in *pb.AuthRoleDeleteRequest, _ ...grpc.CallOption) (*pb.AuthRoleDeleteResponse, error) {
	return c.AuthServer.RoleDelete(ctx, in)
}

func (c serverClient) RoleGrantPermission(ctx context.Context, in *pb.AuthRoleGrantPermissionRequest, _ ...grpc.CallOption) (*pb.AuthRoleGrantPermissionResponse, error) {
	return c.AuthServer.RoleGrantPermission(ctx, in)
}

func (c serverClient) RoleRevokePermission(ctx context.Context, in *pb.AuthRoleRevokePermissionRequest, _ ...grpc.CallOption) (*pb.AuthRoleRevokePermissionResponse, error) {
	return c.AuthServer.RoleRevokePermission(ctx, in)
}
