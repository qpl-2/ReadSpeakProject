package global

import "net"

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

func (qd *User) Domessage(msg string) {
	qd.server.BroadCast(qd, msg)
}

// 监听当前userchannel的方法,有消息就给对面客户端
func (listen *User) ListenUserChannel() {
	for {
		msg := <-listen.C

		listen.conn.Write([]byte(msg + "\n"))
	}
}
