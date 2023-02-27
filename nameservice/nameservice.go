package nameservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jiangdamalong/go-common/log"
	"go.etcd.io/etcd/client"
)

const DefaultSetID = 1 // 供仅作为client的服务使用，Server端需要手动配置

type ServerInfo struct {
	ClusterID    int    `json:"-"`
	SetID        int    `json:"SetID"`
	ServerID     int    `json:"-"`
	GroupID      int    `json:"-"`
	GroupSize    int    `json:"GroupSize"`
	RegisterIP   string `json:"Ip"`
	RegisterPort int    `json:"Port"`
	HttpPort     int    `json:"HttpPort"`
	NewFrame     bool   `json:"new_frame"`
}

func (si ServerInfo) String() string {
	return fmt.Sprintf("{ServerID=%d groupID=%d groupSize=%d RegisterIP=%s RegisterPort=%d HttpPort:%d NewFrame=%v, SetID=%d}",
		si.ServerID, si.GroupID, si.GroupSize, si.RegisterIP, si.RegisterPort, si.HttpPort, si.NewFrame, si.SetID)
}

type Config struct {
	Endpoints     []string      `json:"EtcdClusterServers"` // 和C++配置项对齐
	HeaderTimeout time.Duration `json:"EtcdHeaderTimeout"`
	RootPath      string        `json:"EtcdRootPath"`
	RegisterTTL   time.Duration `json:"EtcdRegisterTTL"`
}

type NameService struct {
	Config
	*NodeCache
	ctx      context.Context
	cancel   context.CancelFunc
	client   client.Client
	mu       sync.RWMutex
	si       ServerInfo // 如果需要注册etcd,则需要
	reigster bool       // 是否注册过
}

func New(config Config) *NameService {
	ns := &NameService{
		Config:    config,
		NodeCache: NewNodeCache(),
	}
	ns.ctx, ns.cancel = context.WithCancel(context.Background())
	return ns
}

func (ns *NameService) registerPath() string {
	ns.mu.RLock()
	s := fmt.Sprintf("%s/%d/%d/%d", ns.RootPath, ns.si.ClusterID, ns.si.GroupID, ns.si.ServerID)
	ns.mu.RUnlock()
	return s
}

func (ns *NameService) registerValue() string {
	value := ""
	ns.mu.RLock()
	if data, err := json.Marshal(&ns.si); err == nil {
		value = string(data)
	} else {
		log.Errorf("marshal server info err:%v", err)
	}
	ns.mu.RUnlock()
	return value
}

func (ns *NameService) serverInfo() ServerInfo {
	ns.mu.RLock()
	s := ns.si
	ns.mu.RUnlock()
	return s
}

func (ns *NameService) setServerInfo(s ServerInfo) {
	ns.mu.Lock()
	ns.si = s
	ns.reigster = true
	ns.mu.Unlock()
}

func (ns *NameService) Init() error {

	endpoints := make([]string, 0)
	for _, endpoint := range ns.Endpoints {
		if strings.Index(endpoint, "http://") == 0 {
			endpoints = append(endpoints, endpoint)
		} else {
			endpoints = append(endpoints, fmt.Sprintf("http://%s", endpoint))
		}
	}
	cfg := client.Config{
		Endpoints:               endpoints,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: ns.HeaderTimeout,
	}

	etcdClient, err := client.New(cfg)
	if err != nil {
		log.Errorf("create etcd client failed, err:%+v", err)
		return err
	}

	log.Infof("create etcd client success")

	ns.client = etcdClient
	ns.SetClientSetID(DefaultSetID)
	return nil
}

func (ns *NameService) SetClientSetID(setID int) {
	ns.updateSetID(setID)
}

func (ns *NameService) GetSetID() int {
	return ns.NodeCache.getSetID()
}

func (ns *NameService) Stop() {
	ns.cancel()
	ns.UnregisterServer()
}

// 服务端注册后，不能动态变更
func (ns *NameService) RegisterServer(serverInfo ServerInfo) error {

	ns.setServerInfo(serverInfo)

	registerPath := ns.registerPath()
	registerValue := ns.registerValue()
	if registerValue == "" {
		return errors.New("register value is empty")
	}

	kapi := client.NewKeysAPI(ns.client)
	_, err := kapi.Set(ns.ctx, registerPath, registerValue, &client.SetOptions{TTL: ns.RegisterTTL})
	if err != nil {
		log.Errorf("register %s failed, err:%s", registerPath, err)
		return err
	}
	ns.addServerInfo(&serverInfo)
	go ns.refreshTTL()
	return nil
}

func (ns *NameService) UnregisterServer() error {
	if false == ns.reigster {
		return nil
	}

	registerPath := ns.registerPath()
	if registerPath == "" {
		return nil
	}

	log.Infof("unregister path:%s", registerPath)
	ns.cancel()
	kapi := client.NewKeysAPI(ns.client)
	_, err := kapi.Delete(context.Background(), registerPath, nil)
	return err
}

func (ns *NameService) refreshTTL() {

	sleepDuration := ns.RegisterTTL / 3
	kapi := client.NewKeysAPI(ns.client)
	registerPath := ns.registerPath()
	for {
		_, err := kapi.Set(ns.ctx, registerPath, "", &client.SetOptions{TTL: ns.RegisterTTL, Refresh: true, PrevExist: client.PrevExist})
		if err != nil {
			if err == context.Canceled {
				log.Infof("refresh canceled")
				break
			}

			log.Errorf("refresh path:%s failed, err:%s", registerPath, err)

			// 遇到错误重新Set一次，确保节点存在，否则refresh会失败
			if _, err = kapi.Set(ns.ctx, registerPath, ns.registerValue(), &client.SetOptions{TTL: ns.RegisterTTL}); err != nil {
				log.Errorf("set path:%s failed, err:%s", registerPath, err)
			}
		}

		log.Debugf("refresh path:%s ttl:%ds", registerPath, ns.RegisterTTL/time.Second)

		time.Sleep(sleepDuration)
	}
}

func (ns *NameService) Watch(path string, recursive bool) error {
	kapi := client.NewKeysAPI(ns.client)
	resp, err := kapi.Get(ns.ctx, path, &client.GetOptions{Recursive: recursive})
	if err != nil {
		log.Errorf("get %s failed, err%+v", path, err)
		return nil
	}
	ns.updateServerLocal(resp)
	go ns.watch(path, recursive, resp.Index+1, false)
	return nil
}

func (ns *NameService) watch(path string, recursive bool, afterIndex uint64, getFlag bool) {
	kapi := client.NewKeysAPI(ns.client)

	// 用非递归实现，如果用递归可能导致异常时调用栈过深
	for {
		if getFlag {
			// 获取全量数据，得到需要watch的最新的index
			// 如果Get失败，则等待一段时间后继续Get，必须Get成功后才执行watch
			// 避免watch到重复事件
			resp, err := kapi.Get(ns.ctx, path, &client.GetOptions{Recursive: recursive})
			if err != nil {
				log.Errorf("get %s failed, err:%s", path, err)
				if err == context.Canceled {
					log.Infof("watch canceled")
					return
				}
				time.Sleep(time.Second * 5)
				continue
			} else {
				afterIndex = resp.Index + 1
				ns.updateServerLocal(resp)
			}
		}

		watcher := kapi.Watcher(path, &client.WatcherOptions{AfterIndex: afterIndex, Recursive: recursive})
		for {
			resp, err := watcher.Next(ns.ctx)
			if err != nil {
				log.Errorf("watch %s failed, err:%s", path, err)
				if err == context.Canceled {
					return
				}
				getFlag = true
				break
			}
			ns.updateServerLocal(resp)
		}
	}
}

// 根据etcd返回信息更新本地缓存
func (ns *NameService) updateServerLocal(resp *client.Response) {
	log.Infof("update node:%s, action:%s", resp.Node.Key, resp.Action)
	switch resp.Action {
	case "get":
		fallthrough
	case "set":
		fallthrough
	case "update":
		fallthrough
	case "create":
		ns.updateNode(resp.Node, true)
	case "expire":
		fallthrough
	case "delete":
		ns.updateNode(resp.Node, false)
	default:
	}
}

func (ns *NameService) updateNode(node *client.Node, addFlag bool) {
	if node.Dir {
		for _, child := range node.Nodes {
			ns.updateNode(child, addFlag)
		}
		return
	}

	log.Debugf("update node:%s value:%s", node.Key, node.Value)

	if serverInfo := ns.analyzeServerInfo(node); serverInfo != nil {
		if addFlag {
			ns.addServerInfo(serverInfo)
		} else {
			ns.delServerInfo(serverInfo.GroupID, serverInfo.ServerID)
		}
	}
}

func (ns *NameService) analyzeServerInfo(node *client.Node) *ServerInfo {
	if node.Dir {
		return nil
	}

	var err error
	serverInfo := &ServerInfo{}

	keyFields := strings.Split(node.Key, "/")
	if len(keyFields) < 3 {
		log.Errorf("invalid key:%s, fields number less than 3", node.Key)
		return nil
	}
	if serverInfo.ClusterID, err = strconv.Atoi(keyFields[len(keyFields)-3]); err != nil {
		log.Errorf("invalie key:%s, ClusterID field not number", node.Key)
		return nil
	}
	if serverInfo.GroupID, err = strconv.Atoi(keyFields[len(keyFields)-2]); err != nil {
		log.Errorf("invalie key:%s, GroupID field not number", node.Key)
		return nil
	}
	if serverInfo.ServerID, err = strconv.Atoi(keyFields[len(keyFields)-1]); err != nil {
		log.Errorf("invalie key:%s, serverID field not number", node.Key)
		return nil
	}
	if serverInfo.ServerID < serverInfo.GroupID {
		log.Errorf("invalie key:%s, invalid serverID and groupID", node.Key)
		return nil
	}

	if node.Value != "" {
		if err := json.Unmarshal([]byte(node.Value), serverInfo); err != nil {
			log.Errorf("unmarshal value failed, key:%s, value:%s, err:%s", node.Key, node.Value, err)
			return nil
		}
	}
	return serverInfo
}
