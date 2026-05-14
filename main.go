package main

import "ReadSpeakProject/global"

func main() {
	server := global.Newserver("120.0.0.1", 8888)
	server.Start()
}
