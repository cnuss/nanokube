package storage

import (
	"context"
	"fmt"
	"sync"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3rpc"
	"google.golang.org/grpc"
)

type ClientImpl struct {
	pb.KVServer
	pb.WatchServer
	pb.LeaseServer
	pb.MaintenanceServer
	pb.ClusterServer
	pb.AuthServer

	ctx context.Context

	serverProvided chan struct{}

	kubernetesOnce sync.Once
	kubernetes     *kubernetes.Client
}

func NewClient(ctx context.Context) v1.StorageClient {
	client := &ClientImpl{
		ctx:            ctx,
		serverProvided: make(chan struct{}),
	}
	return client
}

func (c *ClientImpl) WithServer(s *etcdserver.EtcdServer) v1.StorageClient {
	c.KVServer = v3rpc.NewKVServer(s)
	c.WatchServer = v3rpc.NewWatchServer(s)
	c.LeaseServer = v3rpc.NewLeaseServer(s)
	c.MaintenanceServer = v3rpc.NewMaintenanceServer(s, nil)
	c.ClusterServer = v3rpc.NewClusterServer(s)
	c.AuthServer = v3rpc.NewAuthServer(s)
	close(c.serverProvided)
	return c
}

func (c *ClientImpl) Kubernetes() *kubernetes.Client {
	c.kubernetesOnce.Do(func() {
		<-await(c.ctx, c.serverProvided)
		cli := clientv3.NewCtxClient(c.ctx)
		cli.KV = clientv3.NewKVFromKVClient(c, cli)
		cli.Lease = noopCloseLease{clientv3.NewLeaseFromLeaseClient(c, cli, 0)}
		cli.Watcher = noopCloseWatcher{clientv3.NewWatchFromWatchClient(c, cli)}
		cli.Maintenance = clientv3.NewMaintenanceFromMaintenanceClient(c, cli)
		cli.Cluster = clientv3.NewClusterFromClusterClient(c, cli)
		cli.Auth = clientv3.NewAuthFromAuthClient(c, cli)

		kc := &kubernetes.Client{Client: cli}
		kc.Kubernetes = kc
		c.kubernetes = kc
	})
	return c.kubernetes
}

var (
	_ pb.KVClient          = &ClientImpl{}
	_ pb.WatchClient       = &ClientImpl{}
	_ pb.LeaseClient       = &ClientImpl{}
	_ pb.MaintenanceClient = &ClientImpl{}
	_ pb.ClusterClient     = &ClientImpl{}
	_ pb.AuthClient        = &ClientImpl{}
)

type noopCloseWatcher struct{ clientv3.Watcher }

func (noopCloseWatcher) Close() error { return nil }

type noopCloseLease struct{ clientv3.Lease }

func (noopCloseLease) Close() error { return nil }

// --- KV ---

func (c *ClientImpl) Range(ctx context.Context, in *pb.RangeRequest, _ ...grpc.CallOption) (*pb.RangeResponse, error) {
	return c.KVServer.Range(ctx, in)
}

func (c *ClientImpl) Put(ctx context.Context, in *pb.PutRequest, _ ...grpc.CallOption) (*pb.PutResponse, error) {
	return c.KVServer.Put(ctx, in)
}

func (c *ClientImpl) DeleteRange(ctx context.Context, in *pb.DeleteRangeRequest, _ ...grpc.CallOption) (*pb.DeleteRangeResponse, error) {
	return c.KVServer.DeleteRange(ctx, in)
}

func (c *ClientImpl) Txn(ctx context.Context, in *pb.TxnRequest, _ ...grpc.CallOption) (*pb.TxnResponse, error) {
	return c.KVServer.Txn(ctx, in)
}

func (c *ClientImpl) Compact(ctx context.Context, in *pb.CompactionRequest, _ ...grpc.CallOption) (*pb.CompactionResponse, error) {
	return c.KVServer.Compact(ctx, in)
}

// --- Watch (bidi stream) ---

func (c *ClientImpl) Watch(ctx context.Context, _ ...grpc.CallOption) (pb.Watch_WatchClient, error) {
	return spawnStream(ctx, func(s bidiServer[pb.WatchRequest, pb.WatchResponse]) error {
		return c.WatchServer.Watch(s)
	}), nil
}

// --- Lease ---

func (c *ClientImpl) LeaseGrant(ctx context.Context, in *pb.LeaseGrantRequest, _ ...grpc.CallOption) (*pb.LeaseGrantResponse, error) {
	return c.LeaseServer.LeaseGrant(ctx, in)
}

func (c *ClientImpl) LeaseRevoke(ctx context.Context, in *pb.LeaseRevokeRequest, _ ...grpc.CallOption) (*pb.LeaseRevokeResponse, error) {
	return c.LeaseServer.LeaseRevoke(ctx, in)
}

func (c *ClientImpl) LeaseTimeToLive(ctx context.Context, in *pb.LeaseTimeToLiveRequest, _ ...grpc.CallOption) (*pb.LeaseTimeToLiveResponse, error) {
	return c.LeaseServer.LeaseTimeToLive(ctx, in)
}

func (c *ClientImpl) LeaseLeases(ctx context.Context, in *pb.LeaseLeasesRequest, _ ...grpc.CallOption) (*pb.LeaseLeasesResponse, error) {
	return c.LeaseServer.LeaseLeases(ctx, in)
}

func (c *ClientImpl) LeaseKeepAlive(ctx context.Context, _ ...grpc.CallOption) (pb.Lease_LeaseKeepAliveClient, error) {
	return spawnStream(ctx, func(s bidiServer[pb.LeaseKeepAliveRequest, pb.LeaseKeepAliveResponse]) error {
		return c.LeaseServer.LeaseKeepAlive(s)
	}), nil
}

// --- Maintenance (Snapshot is server-stream; apiserver never calls it) ---

func (c *ClientImpl) Alarm(ctx context.Context, in *pb.AlarmRequest, _ ...grpc.CallOption) (*pb.AlarmResponse, error) {
	return c.MaintenanceServer.Alarm(ctx, in)
}

func (c *ClientImpl) Status(ctx context.Context, in *pb.StatusRequest, _ ...grpc.CallOption) (*pb.StatusResponse, error) {
	return c.MaintenanceServer.Status(ctx, in)
}

func (c *ClientImpl) Defragment(ctx context.Context, in *pb.DefragmentRequest, _ ...grpc.CallOption) (*pb.DefragmentResponse, error) {
	return c.MaintenanceServer.Defragment(ctx, in)
}

func (c *ClientImpl) Hash(ctx context.Context, in *pb.HashRequest, _ ...grpc.CallOption) (*pb.HashResponse, error) {
	return c.MaintenanceServer.Hash(ctx, in)
}

func (c *ClientImpl) HashKV(ctx context.Context, in *pb.HashKVRequest, _ ...grpc.CallOption) (*pb.HashKVResponse, error) {
	return c.MaintenanceServer.HashKV(ctx, in)
}

func (c *ClientImpl) MoveLeader(ctx context.Context, in *pb.MoveLeaderRequest, _ ...grpc.CallOption) (*pb.MoveLeaderResponse, error) {
	return c.MaintenanceServer.MoveLeader(ctx, in)
}

func (c *ClientImpl) Downgrade(ctx context.Context, in *pb.DowngradeRequest, _ ...grpc.CallOption) (*pb.DowngradeResponse, error) {
	return c.MaintenanceServer.Downgrade(ctx, in)
}

func (c *ClientImpl) Snapshot(ctx context.Context, in *pb.SnapshotRequest, _ ...grpc.CallOption) (pb.Maintenance_SnapshotClient, error) {
	return nil, fmt.Errorf("nanokube: in-process etcd snapshot streaming not supported")
}

// --- Cluster ---

func (c *ClientImpl) MemberAdd(ctx context.Context, in *pb.MemberAddRequest, _ ...grpc.CallOption) (*pb.MemberAddResponse, error) {
	return c.ClusterServer.MemberAdd(ctx, in)
}

func (c *ClientImpl) MemberRemove(ctx context.Context, in *pb.MemberRemoveRequest, _ ...grpc.CallOption) (*pb.MemberRemoveResponse, error) {
	return c.ClusterServer.MemberRemove(ctx, in)
}

func (c *ClientImpl) MemberUpdate(ctx context.Context, in *pb.MemberUpdateRequest, _ ...grpc.CallOption) (*pb.MemberUpdateResponse, error) {
	return c.ClusterServer.MemberUpdate(ctx, in)
}

func (c *ClientImpl) MemberList(ctx context.Context, in *pb.MemberListRequest, _ ...grpc.CallOption) (*pb.MemberListResponse, error) {
	return c.ClusterServer.MemberList(ctx, in)
}

func (c *ClientImpl) MemberPromote(ctx context.Context, in *pb.MemberPromoteRequest, _ ...grpc.CallOption) (*pb.MemberPromoteResponse, error) {
	return c.ClusterServer.MemberPromote(ctx, in)
}

// --- Auth (apiserver never enables auth; present only to keep the client whole) ---

func (c *ClientImpl) AuthEnable(ctx context.Context, in *pb.AuthEnableRequest, _ ...grpc.CallOption) (*pb.AuthEnableResponse, error) {
	return c.AuthServer.AuthEnable(ctx, in)
}

func (c *ClientImpl) AuthDisable(ctx context.Context, in *pb.AuthDisableRequest, _ ...grpc.CallOption) (*pb.AuthDisableResponse, error) {
	return c.AuthServer.AuthDisable(ctx, in)
}

func (c *ClientImpl) AuthStatus(ctx context.Context, in *pb.AuthStatusRequest, _ ...grpc.CallOption) (*pb.AuthStatusResponse, error) {
	return c.AuthServer.AuthStatus(ctx, in)
}

func (c *ClientImpl) Authenticate(ctx context.Context, in *pb.AuthenticateRequest, _ ...grpc.CallOption) (*pb.AuthenticateResponse, error) {
	return c.AuthServer.Authenticate(ctx, in)
}

func (c *ClientImpl) UserAdd(ctx context.Context, in *pb.AuthUserAddRequest, _ ...grpc.CallOption) (*pb.AuthUserAddResponse, error) {
	return c.AuthServer.UserAdd(ctx, in)
}

func (c *ClientImpl) UserGet(ctx context.Context, in *pb.AuthUserGetRequest, _ ...grpc.CallOption) (*pb.AuthUserGetResponse, error) {
	return c.AuthServer.UserGet(ctx, in)
}

func (c *ClientImpl) UserList(ctx context.Context, in *pb.AuthUserListRequest, _ ...grpc.CallOption) (*pb.AuthUserListResponse, error) {
	return c.AuthServer.UserList(ctx, in)
}

func (c *ClientImpl) UserDelete(ctx context.Context, in *pb.AuthUserDeleteRequest, _ ...grpc.CallOption) (*pb.AuthUserDeleteResponse, error) {
	return c.AuthServer.UserDelete(ctx, in)
}

func (c *ClientImpl) UserChangePassword(ctx context.Context, in *pb.AuthUserChangePasswordRequest, _ ...grpc.CallOption) (*pb.AuthUserChangePasswordResponse, error) {
	return c.AuthServer.UserChangePassword(ctx, in)
}

func (c *ClientImpl) UserGrantRole(ctx context.Context, in *pb.AuthUserGrantRoleRequest, _ ...grpc.CallOption) (*pb.AuthUserGrantRoleResponse, error) {
	return c.AuthServer.UserGrantRole(ctx, in)
}

func (c *ClientImpl) UserRevokeRole(ctx context.Context, in *pb.AuthUserRevokeRoleRequest, _ ...grpc.CallOption) (*pb.AuthUserRevokeRoleResponse, error) {
	return c.AuthServer.UserRevokeRole(ctx, in)
}

func (c *ClientImpl) RoleAdd(ctx context.Context, in *pb.AuthRoleAddRequest, _ ...grpc.CallOption) (*pb.AuthRoleAddResponse, error) {
	return c.AuthServer.RoleAdd(ctx, in)
}

func (c *ClientImpl) RoleGet(ctx context.Context, in *pb.AuthRoleGetRequest, _ ...grpc.CallOption) (*pb.AuthRoleGetResponse, error) {
	return c.AuthServer.RoleGet(ctx, in)
}

func (c *ClientImpl) RoleList(ctx context.Context, in *pb.AuthRoleListRequest, _ ...grpc.CallOption) (*pb.AuthRoleListResponse, error) {
	return c.AuthServer.RoleList(ctx, in)
}

func (c *ClientImpl) RoleDelete(ctx context.Context, in *pb.AuthRoleDeleteRequest, _ ...grpc.CallOption) (*pb.AuthRoleDeleteResponse, error) {
	return c.AuthServer.RoleDelete(ctx, in)
}

func (c *ClientImpl) RoleGrantPermission(ctx context.Context, in *pb.AuthRoleGrantPermissionRequest, _ ...grpc.CallOption) (*pb.AuthRoleGrantPermissionResponse, error) {
	return c.AuthServer.RoleGrantPermission(ctx, in)
}

func (c *ClientImpl) RoleRevokePermission(ctx context.Context, in *pb.AuthRoleRevokePermissionRequest, _ ...grpc.CallOption) (*pb.AuthRoleRevokePermissionResponse, error) {
	return c.AuthServer.RoleRevokePermission(ctx, in)
}
