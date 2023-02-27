package redisdao

import (
	"encoding/json"
	"io/ioutil"

	"github.com/jiangdamalong/go-common/log"
)

type redisConf struct {
	Addr           string `json:"Addr"`
	Password       string `json:"Password"`
	IdleTimeout    int    `json:"IdleTimeout"`
	MaxIdleCount   int    `json:"MaxIdleCount"`
	MaxActiveCount int    `json:"MaxActiveCount"`
}

var defaultRedisConf = redisConf{
	Addr:           "",
	Password:       "",
	IdleTimeout:    30,
	MaxIdleCount:   50,
	MaxActiveCount: 200,
}

func (conf *redisConf) LoadConfig(file string) error {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Errorf("read file:%s error:%+v", file, err)
		return err
	}
	err = json.Unmarshal(data, conf)
	if err != nil {
		log.Errorf("umarshal data:%+v err:%+v", string(data), err)
		return err
	}
	return nil
}
