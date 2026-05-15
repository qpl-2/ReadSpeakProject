package global

import (
	"net"
	"strings"
)

type User struct {
	Name string
	Addr string
	C    chan string
	conn net.Conn

	server *Server
}

// 创建一个用户的API
func NewUser(conn net.Conn, server *Server) *User {
	userAddr := conn.RemoteAddr().String() //拿到当前客户端链接的地址转string

	user := &User{
		Name: userAddr,
		Addr: userAddr,
		C:    make(chan string),
		conn: conn,

		server: server,
	}

	//启动监听当前userchannel消息的go程
	go user.ListenUserChannel()

	return user
}

func (qd *User) Online() {
	qd.server.mapLock.Lock()
	qd.server.OnlineMap[qd.Name] = qd
	qd.server.mapLock.Unlock()
	//广播用户上线消息
	qd.server.BroadCast(qd, "上线")
}

func (qd *User) Offline() {
	qd.server.mapLock.Lock()
	delete(qd.server.OnlineMap, qd.Name)
	qd.server.mapLock.Unlock()
	//广播用户下线消息
	qd.server.BroadCast(qd, "下线")
}

// 给当前User对应的客户端发送消息
func (fs *User) Sendmsg(msg string) {
	fs.conn.Write([]byte(msg))
}

// 用户处理消息的业务
func (qd *User) Domessage(msg string) {
	if msg == "who" {
		//查询当前在线用户
		qd.server.mapLock.Lock()
		for _, user := range qd.server.OnlineMap {
			onlineMsg := "[" + user.Addr + "]" + user.Name + ":" + "在线...\n"
			qd.Sendmsg(onlineMsg)
		}
		qd.server.mapLock.Unlock()
	} else if len(msg) > 7 && msg[:7] == "rename" {
		//rename|xx
		newName := strings.Split(msg, "|")[1]

		//判断name是否存在
		_, ok := qd.server.OnlineMap[newName]
		if ok {
			qd.Sendmsg("当前用户名存在")
		} else {
			qd.server.mapLock.Lock()
			delete(qd.server.OnlineMap, qd.Name)
			qd.server.OnlineMap[newName] = qd
			qd.server.mapLock.Unlock()

			qd.Name = newName
			qd.Sendmsg("更新用户名成功" + qd.Name + "\n")

		}
	} else {
		qd.server.BroadCast(qd, msg)
	}
	qd.server.BroadCast(qd, msg)
}

// 监听当前userchannel的方法,有消息就给对面客户端
func (listen *User) ListenUserChannel() {
	for {
		msg := <-listen.C

		listen.conn.Write([]byte(msg + "\n"))
	}
}
