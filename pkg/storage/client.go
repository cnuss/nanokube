package storage

import (
	"context"
	"fmt"
	"sync"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	kineserver "github.com/k3s-io/kine/pkg/server"
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

	kvProvided          chan struct{}
	watchProvided       chan struct{}
	leaseProvided       chan struct{}
	maintenanceProvided chan struct{}
	clusterProvided     chan struct{}
	authProvided        chan struct{}

	kubernetesOnce sync.Once
	kubernetes     *kubernetes.Client
}

func NewClient(ctx context.Context) v1.StorageClient {
	client := &ClientImpl{
		ctx:                 ctx,
		kvProvided:          make(chan struct{}),
		watchProvided:       make(chan struct{}),
		leaseProvided:       make(chan struct{}),
		maintenanceProvided: make(chan struct{}),
		clusterProvided:     make(chan struct{}),
		authProvided:        make(chan struct{}),
	}
	return client
}

func (c *ClientImpl) WithServer(s *etcdserver.EtcdServer) v1.StorageClient {
	return c.WithKV(v3rpc.NewKVServer(s)).
		WithWatch(v3rpc.NewWatchServer(s)).
		WithLease(v3rpc.NewLeaseServer(s)).
		WithMaintenance(v3rpc.NewMaintenanceServer(s, nil)).
		WithCluster(v3rpc.NewClusterServer(s)).
		WithAuth(v3rpc.NewAuthServer(s))
}

func (c *ClientImpl) WithBridge(b *kineserver.KVServerBridge) v1.StorageClient {
	return c.WithKV(b).
		WithWatch(b).
		WithLease(b).
		WithMaintenance(b).
		WithCluster(b).
		WithAuth(nil) // kine has no AuthServer; close the gate so auth calls fail fast instead of blocking
}

func (c *ClientImpl) WithKV(kv pb.KVServer) v1.StorageClient {
	c.KVServer = kv
	close(c.kvProvided)
	return c
}

func (c *ClientImpl) WithWatch(watch pb.WatchServer) v1.StorageClient {
	c.WatchServer = watch
	close(c.watchProvided)
	return c
}

func (c *ClientImpl) WithLease(lease pb.LeaseServer) v1.StorageClient {
	c.LeaseServer = lease
	close(c.leaseProvided)
	return c
}

func (c *ClientImpl) WithMaintenance(maintenance pb.MaintenanceServer) v1.StorageClient {
	c.MaintenanceServer = maintenance
	close(c.maintenanceProvided)
	return c
}

func (c *ClientImpl) WithCluster(cluster pb.ClusterServer) v1.StorageClient {
	c.ClusterServer = cluster
	close(c.clusterProvided)
	return c
}

func (c *ClientImpl) WithAuth(auth pb.AuthServer) v1.StorageClient {
	c.AuthServer = auth
	close(c.authProvided)
	return c
}

func (c *ClientImpl) Kubernetes() *kubernetes.Client {
	c.kubernetesOnce.Do(func() {
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

func (c *ClientImpl) Etcd() *clientv3.Client {
	return c.Kubernetes().Client
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
	await(ctx, c.kvProvided)
	return c.KVServer.Range(ctx, in)
}

func (c *ClientImpl) Put(ctx context.Context, in *pb.PutRequest, _ ...grpc.CallOption) (*pb.PutResponse, error) {
	await(ctx, c.kvProvided)
	return c.KVServer.Put(ctx, in)
}

func (c *ClientImpl) DeleteRange(ctx context.Context, in *pb.DeleteRangeRequest, _ ...grpc.CallOption) (*pb.DeleteRangeResponse, error) {
	await(ctx, c.kvProvided)
	return c.KVServer.DeleteRange(ctx, in)
}

func (c *ClientImpl) Txn(ctx context.Context, in *pb.TxnRequest, _ ...grpc.CallOption) (*pb.TxnResponse, error) {
	await(ctx, c.kvProvided)
	return c.KVServer.Txn(ctx, in)
}

func (c *ClientImpl) Compact(ctx context.Context, in *pb.CompactionRequest, _ ...grpc.CallOption) (*pb.CompactionResponse, error) {
	await(ctx, c.kvProvided)
	return c.KVServer.Compact(ctx, in)
}

// --- Watch (bidi stream) ---

func (c *ClientImpl) Watch(ctx context.Context, _ ...grpc.CallOption) (pb.Watch_WatchClient, error) {
	return spawnStream(ctx, func(s bidiServer[pb.WatchRequest, pb.WatchResponse]) error {
		await(ctx, c.watchProvided)
		return c.WatchServer.Watch(s)
	}), nil
}

// --- Lease ---

func (c *ClientImpl) LeaseGrant(ctx context.Context, in *pb.LeaseGrantRequest, _ ...grpc.CallOption) (*pb.LeaseGrantResponse, error) {
	await(ctx, c.leaseProvided)
	return c.LeaseServer.LeaseGrant(ctx, in)
}

func (c *ClientImpl) LeaseRevoke(ctx context.Context, in *pb.LeaseRevokeRequest, _ ...grpc.CallOption) (*pb.LeaseRevokeResponse, error) {
	await(ctx, c.leaseProvided)
	return c.LeaseServer.LeaseRevoke(ctx, in)
}

func (c *ClientImpl) LeaseTimeToLive(ctx context.Context, in *pb.LeaseTimeToLiveRequest, _ ...grpc.CallOption) (*pb.LeaseTimeToLiveResponse, error) {
	await(ctx, c.leaseProvided)
	return c.LeaseServer.LeaseTimeToLive(ctx, in)
}

func (c *ClientImpl) LeaseLeases(ctx context.Context, in *pb.LeaseLeasesRequest, _ ...grpc.CallOption) (*pb.LeaseLeasesResponse, error) {
	await(ctx, c.leaseProvided)
	return c.LeaseServer.LeaseLeases(ctx, in)
}

func (c *ClientImpl) LeaseKeepAlive(ctx context.Context, _ ...grpc.CallOption) (pb.Lease_LeaseKeepAliveClient, error) {
	return spawnStream(ctx, func(s bidiServer[pb.LeaseKeepAliveRequest, pb.LeaseKeepAliveResponse]) error {
		await(ctx, c.leaseProvided)
		return c.LeaseServer.LeaseKeepAlive(s)
	}), nil
}

// --- Maintenance (Snapshot is server-stream; apiserver never calls it) ---

func (c *ClientImpl) Alarm(ctx context.Context, in *pb.AlarmRequest, _ ...grpc.CallOption) (*pb.AlarmResponse, error) {
	await(ctx, c.maintenanceProvided)
	return c.MaintenanceServer.Alarm(ctx, in)
}

func (c *ClientImpl) Status(ctx context.Context, in *pb.StatusRequest, _ ...grpc.CallOption) (*pb.StatusResponse, error) {
	await(ctx, c.maintenanceProvided)
	return c.MaintenanceServer.Status(ctx, in)
}

func (c *ClientImpl) Defragment(ctx context.Context, in *pb.DefragmentRequest, _ ...grpc.CallOption) (*pb.DefragmentResponse, error) {
	await(ctx, c.maintenanceProvided)
	return c.MaintenanceServer.Defragment(ctx, in)
}

func (c *ClientImpl) Hash(ctx context.Context, in *pb.HashRequest, _ ...grpc.CallOption) (*pb.HashResponse, error) {
	await(ctx, c.maintenanceProvided)
	return c.MaintenanceServer.Hash(ctx, in)
}

func (c *ClientImpl) HashKV(ctx context.Context, in *pb.HashKVRequest, _ ...grpc.CallOption) (*pb.HashKVResponse, error) {
	await(ctx, c.maintenanceProvided)
	return c.MaintenanceServer.HashKV(ctx, in)
}

func (c *ClientImpl) MoveLeader(ctx context.Context, in *pb.MoveLeaderRequest, _ ...grpc.CallOption) (*pb.MoveLeaderResponse, error) {
	await(ctx, c.maintenanceProvided)
	return c.MaintenanceServer.MoveLeader(ctx, in)
}

func (c *ClientImpl) Downgrade(ctx context.Context, in *pb.DowngradeRequest, _ ...grpc.CallOption) (*pb.DowngradeResponse, error) {
	await(ctx, c.maintenanceProvided)
	return c.MaintenanceServer.Downgrade(ctx, in)
}

func (c *ClientImpl) Snapshot(ctx context.Context, in *pb.SnapshotRequest, _ ...grpc.CallOption) (pb.Maintenance_SnapshotClient, error) {
	// TODO(partial): support snapshot
	return nil, fmt.Errorf("nanokube: in-process etcd snapshot streaming not supported")
}

// --- Cluster ---

func (c *ClientImpl) MemberAdd(ctx context.Context, in *pb.MemberAddRequest, _ ...grpc.CallOption) (*pb.MemberAddResponse, error) {
	await(ctx, c.clusterProvided)
	return c.ClusterServer.MemberAdd(ctx, in)
}

func (c *ClientImpl) MemberRemove(ctx context.Context, in *pb.MemberRemoveRequest, _ ...grpc.CallOption) (*pb.MemberRemoveResponse, error) {
	await(ctx, c.clusterProvided)
	return c.ClusterServer.MemberRemove(ctx, in)
}

func (c *ClientImpl) MemberUpdate(ctx context.Context, in *pb.MemberUpdateRequest, _ ...grpc.CallOption) (*pb.MemberUpdateResponse, error) {
	await(ctx, c.clusterProvided)
	return c.ClusterServer.MemberUpdate(ctx, in)
}

func (c *ClientImpl) MemberList(ctx context.Context, in *pb.MemberListRequest, _ ...grpc.CallOption) (*pb.MemberListResponse, error) {
	await(ctx, c.clusterProvided)
	return c.ClusterServer.MemberList(ctx, in)
}

func (c *ClientImpl) MemberPromote(ctx context.Context, in *pb.MemberPromoteRequest, _ ...grpc.CallOption) (*pb.MemberPromoteResponse, error) {
	await(ctx, c.clusterProvided)
	return c.ClusterServer.MemberPromote(ctx, in)
}

// --- Auth (apiserver never enables auth; present only to keep the client whole) ---

func (c *ClientImpl) AuthEnable(ctx context.Context, in *pb.AuthEnableRequest, _ ...grpc.CallOption) (*pb.AuthEnableResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.AuthEnable(ctx, in)
}

func (c *ClientImpl) AuthDisable(ctx context.Context, in *pb.AuthDisableRequest, _ ...grpc.CallOption) (*pb.AuthDisableResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.AuthDisable(ctx, in)
}

func (c *ClientImpl) AuthStatus(ctx context.Context, in *pb.AuthStatusRequest, _ ...grpc.CallOption) (*pb.AuthStatusResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.AuthStatus(ctx, in)
}

func (c *ClientImpl) Authenticate(ctx context.Context, in *pb.AuthenticateRequest, _ ...grpc.CallOption) (*pb.AuthenticateResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.Authenticate(ctx, in)
}

func (c *ClientImpl) UserAdd(ctx context.Context, in *pb.AuthUserAddRequest, _ ...grpc.CallOption) (*pb.AuthUserAddResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.UserAdd(ctx, in)
}

func (c *ClientImpl) UserGet(ctx context.Context, in *pb.AuthUserGetRequest, _ ...grpc.CallOption) (*pb.AuthUserGetResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.UserGet(ctx, in)
}

func (c *ClientImpl) UserList(ctx context.Context, in *pb.AuthUserListRequest, _ ...grpc.CallOption) (*pb.AuthUserListResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.UserList(ctx, in)
}

func (c *ClientImpl) UserDelete(ctx context.Context, in *pb.AuthUserDeleteRequest, _ ...grpc.CallOption) (*pb.AuthUserDeleteResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.UserDelete(ctx, in)
}

func (c *ClientImpl) UserChangePassword(ctx context.Context, in *pb.AuthUserChangePasswordRequest, _ ...grpc.CallOption) (*pb.AuthUserChangePasswordResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.UserChangePassword(ctx, in)
}

func (c *ClientImpl) UserGrantRole(ctx context.Context, in *pb.AuthUserGrantRoleRequest, _ ...grpc.CallOption) (*pb.AuthUserGrantRoleResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.UserGrantRole(ctx, in)
}

func (c *ClientImpl) UserRevokeRole(ctx context.Context, in *pb.AuthUserRevokeRoleRequest, _ ...grpc.CallOption) (*pb.AuthUserRevokeRoleResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.UserRevokeRole(ctx, in)
}

func (c *ClientImpl) RoleAdd(ctx context.Context, in *pb.AuthRoleAddRequest, _ ...grpc.CallOption) (*pb.AuthRoleAddResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.RoleAdd(ctx, in)
}

func (c *ClientImpl) RoleGet(ctx context.Context, in *pb.AuthRoleGetRequest, _ ...grpc.CallOption) (*pb.AuthRoleGetResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.RoleGet(ctx, in)
}

func (c *ClientImpl) RoleList(ctx context.Context, in *pb.AuthRoleListRequest, _ ...grpc.CallOption) (*pb.AuthRoleListResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.RoleList(ctx, in)
}

func (c *ClientImpl) RoleDelete(ctx context.Context, in *pb.AuthRoleDeleteRequest, _ ...grpc.CallOption) (*pb.AuthRoleDeleteResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.RoleDelete(ctx, in)
}

func (c *ClientImpl) RoleGrantPermission(ctx context.Context, in *pb.AuthRoleGrantPermissionRequest, _ ...grpc.CallOption) (*pb.AuthRoleGrantPermissionResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.RoleGrantPermission(ctx, in)
}

func (c *ClientImpl) RoleRevokePermission(ctx context.Context, in *pb.AuthRoleRevokePermissionRequest, _ ...grpc.CallOption) (*pb.AuthRoleRevokePermissionResponse, error) {
	await(ctx, c.authProvided)
	return c.AuthServer.RoleRevokePermission(ctx, in)
}
