package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	l, err := net.Listen("tcp", ":6380")
	if err != nil {
		fmt.Println(err)
		return
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}
		if value.typ != "array" {
			fmt.Println("Invalid type:", value.typ)
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		writer := NewWrite(conn)
		handler, ok := Handler[command]
		if !ok {
			fmt.Println("Invalid command:", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		result := handler(args)
		writer.Write(result)
	}
}
