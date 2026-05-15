package global

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type Server struct {
	Ip        string
	Port      int
	OnlineMap map[string]*User
	mapLock   sync.RWMutex
	Message   chan string
}

// 创建一个server的接口
func Newserver(ip string, port int) *Server {
	server := &Server{
		Ip:        ip,
		Port:      port,
		OnlineMap: make(map[string]*User),
		Message:   make(chan string),
	}

	return server
}

// 监听Message广播消息的channel的goroutine，有消息发给全部User
func (jt *Server) ListenMessager() {
	for {
		msg := <-jt.Message

		//msg发送给全部User
		jt.mapLock.Lock()
		for _, client := range jt.OnlineMap {
			client.C <- msg
		}
		jt.mapLock.Unlock()
	}
}

// 广播消息的方法
func (gb *Server) BroadCast(user *User, msg string) {
	sendMsg := "[" + user.Addr + "]" + user.Name + ":" + msg

	gb.Message <- sendMsg
}

func (qd *Server) Handler(conn net.Conn) {
	//当前链接的业务
	user := NewUser(conn, qd)

	user.Online()

	//监听用户是否活跃的channel
	isLive := make(chan bool)
	//接收客户端发送的消息
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if n == 0 {
				qd.BroadCast(user, "下线")
				return
			}
			if err != nil && err == io.EOF {
				fmt.Println("conn读取错误", err)
				return
			}
			//提取用户消息
			msg := string(buf[0 : n-1])
			//广播
			user.Domessage(msg)

			//用户有消息，代表活跃
			isLive <- true
		}
	}()
	//阻塞handler，不然会死亡退出进程
	for {
		select {
		case <-isLive:
			//说明当前用户是活跃的，重置定时器
		case <-time.After(time.Second * 10): //after本质是一个channel
			//超时
			user.Sendmsg("以踢出")
			close(user.C)

			//关闭连接
			conn.Close()

			//退出当前handler
			return //return会结束当前函数中的所以子协程
		}
	}
}

// 启动服务器的接口
func (qd *Server) Start() {
	//监听
	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", qd.Ip, qd.Port))
	if err != nil {
		fmt.Println("net.Listen err:", err)
		return
	}
	//关闭监听
	defer listen.Close()

	//  启动监听message的go程
	go qd.ListenMessager()

	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("Listener Accept err:", err)
			continue
		} //当前循环有可能阻塞,用go程

		go qd.Handler(conn)
	}
}
