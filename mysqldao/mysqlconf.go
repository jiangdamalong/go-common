package mysqldao

import (
	"encoding/json"
	"io/ioutil"

	"github.com/jiangdamalong/go-common/log"
)

type mysqlConf struct {
	Host           string `json:"Host"`
	User           string `json:"User"`
	PassWord       string `json:"PassWord"`
	Db             string `json:"Db"`
	ReadTimeOut    int    `json:"ReadTimeOut"`
	WriteTimeOut   int    `json:"WriteTimeOut"`
	ConnectTimeOut int    `json:"ConnectTimeOut"`
	IdleTimeOut    int    `json:"IdleTimeOut"`
	MaxFreeConnNum int    `json:"MaxFreeConnNum"`
	MaxConnNum     int    `json:"MaxConnNum"`
}

var defaultMysqlConf = mysqlConf{
	Host:           "",
	User:           "",
	PassWord:       "",
	Db:             "",
	ReadTimeOut:    30,
	WriteTimeOut:   10,
	ConnectTimeOut: 10,
	IdleTimeOut:    60 * 10,
	MaxFreeConnNum: 10,
	MaxConnNum:     50,
}

func (mysqlConf *mysqlConf) LoadConfig(file string) error {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Errorf("read file:%s error:%+v", file, err)
		return err
	}
	err = json.Unmarshal(data, mysqlConf)
	if err != nil {
		log.Errorf("umarshal data:%+v err:%+v", string(data), err)
		return err
	}
	return nil
}
