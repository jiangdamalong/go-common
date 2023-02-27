package mysqldao

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLDao struct {
	conn *sql.DB
	conf mysqlConf
}

func New() *MySQLDao {
	mysql := new(MySQLDao)
	mysql.conf = defaultMysqlConf
	return mysql
}

func (mysql *MySQLDao) InitConfig(file string) error {
	return mysql.conf.LoadConfig(file)
}

func (mysql *MySQLDao) SetIdleTimeOut(idleTimeOut int) *MySQLDao {
	mysql.conf.IdleTimeOut = idleTimeOut
	return mysql
}

func (mysql *MySQLDao) SetReadTimeOut(readTimeOut int) *MySQLDao {
	mysql.conf.ReadTimeOut = readTimeOut
	return mysql
}

func (mysql *MySQLDao) SetWriteTimeOut(writeTimeOUt int) *MySQLDao {
	mysql.conf.WriteTimeOut = writeTimeOUt
	return mysql
}

func (mysql *MySQLDao) SetConnectTimeOut(connectTimeOUt int) *MySQLDao {
	mysql.conf.ConnectTimeOut = connectTimeOUt
	return mysql
}

func (mysql *MySQLDao) SetMaxFreeConnNum(maxFreeConnNum int) *MySQLDao {
	mysql.conf.MaxFreeConnNum = maxFreeConnNum
	return mysql
}

func (mysql *MySQLDao) SetMaxConnNum(maxConnNum int) *MySQLDao {
	mysql.conf.MaxConnNum = maxConnNum
	return mysql
}

func (mysql *MySQLDao) InitOpen() error {
	connString := fmt.Sprintf("%s:%s@tcp(%s)/%s?timeout=%ds&writeTimeout=%ds&readTimeout=%ds",
		mysql.conf.User, mysql.conf.PassWord, mysql.conf.Host, mysql.conf.Db, mysql.conf.ConnectTimeOut,
		mysql.conf.WriteTimeOut, mysql.conf.ReadTimeOut)

	dbConn, err := sql.Open("mysql", connString)
	if err != nil {
		return err
	}
	mysql.conn = dbConn
	mysql.conn.SetMaxIdleConns(mysql.conf.MaxFreeConnNum)
	mysql.conn.SetMaxOpenConns(mysql.conf.MaxConnNum)
	mysql.conn.SetConnMaxLifetime(time.Duration(mysql.conf.IdleTimeOut) * time.Second)

	return nil
}

func (mysql *MySQLDao) Close() {
	if mysql.conn != nil {
		mysql.conn.Close()
	}
}

func (mysql *MySQLDao) Conn() *sql.DB {
	return mysql.conn
}
