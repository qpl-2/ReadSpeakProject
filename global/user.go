package global

import "net"

type User struct {
	Name string
	Addr string
	C    chan string
	conn net.Conn
}

// 创建一个用户的API
func NewUser(conn net.Conn) *User {
	userAddr := conn.RemoteAddr().String() //拿到当前客户端链接的地址转string

	user := &User{
		Name: userAddr,
		Addr: userAddr,
		C:    make(chan string),
		conn: conn,
	}

	//启动监听当前userchannel消息的go程
	go user.ListenUserChannel()

	return user
}

// 监听当前userchannel的方法,有消息就给对面客户端
func (listen *User) ListenUserChannel() {
	for {
		msg := <-listen.C

		listen.conn.Write([]byte(msg + "\n"))
	}
}
