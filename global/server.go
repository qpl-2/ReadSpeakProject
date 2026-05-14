package global

import (
	"fmt"
	"net"
)

type Server struct {
	Ip   string
	Port int
}

// 创建一个server的接口
func Newserver(ip string, port int) *Server {
	server := &Server{
		Ip:   ip,
		Port: port,
	}

	return server
}

func (qd *Server) Handler(conn net.Conn) {
	//当前链接的业务
	fmt.Println("链接建立成功")
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
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("Listener Accept err:", err)
			continue
		} //当前循环有可能阻塞,用go程

		go qd.Handler(conn)
	}
}
