package redisdao

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type RedisDao struct {
	pool *redis.Pool
	conf redisConf
}

func New() *RedisDao {

	redisDao := new(RedisDao)
	redisDao.conf = defaultRedisConf

	return redisDao
}

func (redisDao *RedisDao) InitConfig(file string) error {
	return redisDao.conf.LoadConfig(file)
}

func (redisDao *RedisDao) InitDao() error {

	redisDao.pool = &redis.Pool{
		Dial: func() (redis.Conn, error) {
			options := redis.DialPassword(redisDao.conf.Password)
			c, err := redis.Dial("tcp", redisDao.conf.Addr, options)
			if err != nil {
				return nil, err
			}
			return c, err

		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxIdle:     redisDao.conf.MaxIdleCount,
		MaxActive:   redisDao.conf.MaxActiveCount,
		IdleTimeout: time.Second * time.Duration(redisDao.conf.IdleTimeout),
		Wait:        true,
	}

	return nil
}

func (redisDao *RedisDao) Close() {
	redisDao.pool.Close()
}

func (redisDao *RedisDao) GetConn() redis.Conn {
	conn := redisDao.pool.Get()
	return conn
}
