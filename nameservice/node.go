package nameservice

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiangdamalong/go-common/log"
)

const HashKeyInvalid = -1
const MspInvalid = -1
const MspOneRandomlyWithGroup = 1
const MspOneModGroupSize1 = 2 // 根据外部设置大小取模
const MspOneModGroupSize2 = 3 // 根据实际大小取模
const MspMember = 4

func init() {
	rand.Seed(time.Now().Unix())
}

type IntSet struct {
	datas []int
}

func NewIntSet() *IntSet {
	return &IntSet{
		datas: make([]int, 0),
	}
}

func (is *IntSet) Add(data int) {
	is.Del(data)
	is.datas = append(is.datas, data)
	sort.Ints(is.datas)
}

func (is *IntSet) Del(data int) {
	found := -1
	for index, value := range is.datas {
		if data == value {
			found = index
			break
		}
	}

	if found == -1 {
		return
	}

	copy(is.datas[found:len(is.datas)-1], is.datas[found+1:len(is.datas)])
	is.datas = is.datas[0 : len(is.datas)-1]
}

func (is *IntSet) Random() int {
	if is.Len() == 0 {
		return -1
	}
	index := rand.Int() % is.Len()
	return is.datas[index]
}

func (is *IntSet) Hash(key int) int {
	if is.Len() == 0 {
		return -1
	}
	index := key % is.Len()
	return is.datas[index]
}

func (is *IntSet) Len() int {
	return len(is.datas)
}

type NodeCache struct {
	sync.RWMutex
	setID          int64
	serverIndex    map[int]*ServerInfo
	groupIndex     map[int]*IntSet // groupID对应的serverID集合
	groupSizeIndex map[int]int     // groupID对应的groupSize
	groupSetsIndex map[int]*IntSet // groupID对应set集合
}

func NewNodeCache() *NodeCache {
	return &NodeCache{
		serverIndex:    map[int]*ServerInfo{},
		groupIndex:     map[int]*IntSet{},
		groupSizeIndex: map[int]int{},
		groupSetsIndex: map[int]*IntSet{},
	}
}

func makeGroupSetID(groupID, setID int) int {
	return groupID + setID
}

func parseGroupSetID(id int) (int, int) {
	return id / 1000 * 1000, id % 1000
}

// 往m[index]中添加id
func addIntSetItem(m map[int]*IntSet, index, id int) {
	ids, ok := m[index]
	if ok {
		ids.Add(id)
		log.Infof("add id, index:%d, id:%d, len:%d", index, id, ids.Len())
	} else {
		ids := NewIntSet()
		ids.Add(id)
		m[index] = ids
		log.Infof("add index, index:%d, id:%d, len:%d", index, id, ids.Len())
	}
}

func delIntSetItem(m map[int]*IntSet, index, id int) {
	ids, ok := m[index]
	if ok {
		ids.Del(id)
		log.Infof("del id, index:%d, id:%d, len:%d", index, id, ids.Len())
		if ids.Len() == 0 {
			delete(m, index)
			log.Infof("del index, index:%d, id:%d, len:%d", index, id, ids.Len())
		}
	}
}

func (nc *NodeCache) addServerInfoInGroup(gid int, serverInfo *ServerInfo) {
	log.Infof("add server info in group:%d, %s", gid, serverInfo)

	nc.serverIndex[serverInfo.ServerID] = serverInfo

	if serverInfo.GroupSize > 0 {
		log.Debugf("set group size, groupID:%d size:%d", gid, serverInfo.GroupSize)
		nc.groupSizeIndex[gid] = serverInfo.GroupSize
	}

	addIntSetItem(nc.groupIndex, gid, serverInfo.ServerID)
	if gid, setID := parseGroupSetID(gid); setID > 0 {
		addIntSetItem(nc.groupSetsIndex, gid, setID)
	}
}

func (nc *NodeCache) addServerInfo(serverInfo *ServerInfo) {
	nc.Lock()
	defer nc.Unlock()

	// set信息变化，删除原来的set
	si := nc.serverIndex[serverInfo.ServerID]
	if si != nil && si.SetID > 0 && si.SetID != serverInfo.SetID {
		nc.delServerInfoInGroup(makeGroupSetID(si.GroupID, si.SetID), si.ServerID)
	}

	nc.addServerInfoInGroup(serverInfo.GroupID, serverInfo)
	if serverInfo.SetID > 0 {
		nc.addServerInfoInGroup(makeGroupSetID(serverInfo.GroupID, serverInfo.SetID), serverInfo)
	}
}

func (nc *NodeCache) delServerInfoInGroup(gid, sid int) {
	log.Infof("del server info in group:%d, %d", gid, sid)

	delete(nc.serverIndex, sid)
	delIntSetItem(nc.groupIndex, gid, sid)
	if _, ok := nc.groupIndex[gid]; !ok {
		// 如果set内部serverID全部删除，则删除groupID对应的setID映射
		if gid, setID := parseGroupSetID(gid); setID > 0 {
			delIntSetItem(nc.groupSetsIndex, gid, setID)
		}
	}
}

func (nc *NodeCache) delServerInfo(gid, sid int) {
	nc.Lock()
	defer nc.Unlock()

	si := nc.serverIndex[sid]
	if si == nil {
		return
	}

	nc.delServerInfoInGroup(gid, sid)
	if si.SetID > 0 {
		nc.delServerInfoInGroup(makeGroupSetID(si.GroupID, si.SetID), sid)
	}
}

func (nc *NodeCache) updateSetID(setID int) {
	atomic.StoreInt64(&nc.setID, int64(setID))
}

func (nc *NodeCache) getSetID() int {
	return int(nc.setID)
}

// 以下GET类接口供客户端调用
func (nc *NodeCache) GetOneServerByServerID(serverID int) *ServerInfo {
	nc.RLock()
	defer nc.RUnlock()

	s, ok := nc.serverIndex[serverID]
	if ok {
		return s
	} else {
		return nil
	}
}

func (nc *NodeCache) getOneServerByHashInGroup(groupID, hashKey, hashType int) *ServerInfo {
	if hashKey < 0 {
		return nil
	}

	if hashType == MspOneModGroupSize1 {
		groupSize, ok := nc.groupSizeIndex[groupID]
		if !ok {
			return nil
		}
		sid := groupID + hashKey%groupSize + 1
		serverInfo, ok := nc.serverIndex[sid]
		if ok {
			return serverInfo
		} else {
			return nil
		}

	} else if hashType == MspOneModGroupSize2 {
		sids, ok := nc.groupIndex[groupID]
		if !ok {
			return nil
		}
		sid := sids.Hash(hashKey)
		serverInfo, ok := nc.serverIndex[sid]
		if ok {
			return serverInfo
		} else {
			return nil
		}
	} else {
		log.Errorf("invalid hashType:%d", hashType)
		return nil
	}
}

func (nc *NodeCache) GetOneServerByHash(groupID, hashKey, hashType int) *ServerInfo {
	nc.RLock()
	defer nc.RUnlock()

	if groupID%1000 != 0 {
		return nil
	}

	if nc.setID > 0 && hashType != MspOneModGroupSize1 {
		// 如果被调方配置了Set信息，则找相同Set的，如果没找到，则从全部里面找
		newGid := makeGroupSetID(groupID, int(nc.setID))
		if _, ok := nc.groupIndex[newGid]; ok {
			return nc.getOneServerByHashInGroup(newGid, hashKey, hashType)
		}
	}
	return nc.getOneServerByHashInGroup(groupID, hashKey, hashType)
}

func (nc *NodeCache) getOneServerByRandomInGroup(groupID int) *ServerInfo {
	sids, ok := nc.groupIndex[groupID]
	if !ok {
		return nil
	}

	sid := sids.Random()
	serverInfo, ok := nc.serverIndex[sid]
	if ok {
		return serverInfo
	} else {
		return nil
	}
}

func (nc *NodeCache) GetOneServerByRandom(groupID int) *ServerInfo {

	nc.RLock()
	defer nc.RUnlock()

	if groupID%1000 != 0 {
		return nil
	}

	if nc.setID > 0 {
		// 如果被调方配置了Set信息，则找相同Set的，如果没找到，则从全部里面找
		newGid := makeGroupSetID(groupID, int(nc.setID))
		if _, ok := nc.groupIndex[newGid]; ok {
			return nc.getOneServerByRandomInGroup(newGid)
		}
	}
	return nc.getOneServerByRandomInGroup(groupID)
}

func (nc *NodeCache) GetAllServerByGroupID(groupID int) []*ServerInfo {
	nc.RLock()
	defer nc.RUnlock()

	sids, ok := nc.groupIndex[groupID]
	if !ok {
		log.Debugf("groupIndex not found:%d", groupID)
		return nil
	}

	serverInfos := make([]*ServerInfo, 0)
	for _, sid := range sids.datas {
		serverInfo, ok := nc.serverIndex[sid]
		if ok {
			serverInfos = append(serverInfos, serverInfo)
		}
	}
	return serverInfos
}

func (nc *NodeCache) GetAllGroupIDs() []int {
	gids := make([]int, 0)
	for gid, _ := range nc.groupIndex {
		if gid%1000 == 0 {
			gids = append(gids, gid)
		}
	}
	return gids
}

func (nc *NodeCache) GetServerInfo(serverID, sendType, hashKey int) *ServerInfo {
	var serverInfo *ServerInfo

	switch sendType {
	case MspOneRandomlyWithGroup:
		serverInfo = nc.GetOneServerByRandom(serverID)
	case MspOneModGroupSize1, MspOneModGroupSize2:
		serverInfo = nc.GetOneServerByHash(serverID, hashKey, sendType)
	case MspMember:
		serverInfo = nc.GetOneServerByServerID(serverID)
	}

	return serverInfo
}

type GroupInfo struct {
	Sids       []ServerInfo
	GroupSize  int
	GroupIndex map[int][]int
	Sets       []int
}

func (gi *GroupInfo) String() string {
	return fmt.Sprintf("Sids:%v GroupSize:%d Sets:%v, GroupIndex:%v",
		gi.Sids, gi.GroupSize, gi.Sets, gi.GroupIndex)
}

func (nc *NodeCache) DumpGroup(gid int) string {
	nc.RLock()
	defer nc.RUnlock()

	groupInfo := GroupInfo{
		GroupIndex: make(map[int][]int),
	}
	if sids, ok := nc.groupIndex[gid]; ok {
		groupInfo.Sids = make([]ServerInfo, 0, sids.Len())
		groupInfo.GroupIndex[gid] = sids.datas
		for _, sid := range sids.datas {
			if si, ok := nc.serverIndex[sid]; ok {
				groupInfo.Sids = append(groupInfo.Sids, *si)
			}
		}
	}
	groupInfo.GroupSize = nc.groupSizeIndex[gid]
	if sets, ok := nc.groupSetsIndex[gid]; ok {
		groupInfo.Sets = sets.datas
		for _, setID := range sets.datas {
			if sids, ok := nc.groupIndex[makeGroupSetID(gid, setID)]; ok {
				groupInfo.GroupIndex[makeGroupSetID(gid, setID)] = sids.datas
			}
		}
	}
	return fmt.Sprintf("%v", &groupInfo)
}
