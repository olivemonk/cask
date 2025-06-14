package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

const PORT = "6380"

type Store struct {
	lock sync.Mutex
	kv   map[string]string
}

func NewStore() *Store {
	store := &Store{
		kv: make(map[string]string),
	}
	return store
}

func (s *Store) Set(key string, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.kv[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	val, exists := s.kv[key]
	return val, exists
}

func (s *Store) Delete(key string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, exists := s.kv[key]; exists {
		delete(s.kv, key)
		return true
	}
	return false
}

func handleClient(conn net.Conn, store *Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Println("Read error:", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 || line[0] != '*' {
			conn.Write([]byte("-ERR expected array format\r\n"))
			continue
		}

		count, err := strconv.Atoi(line[1:])
		if err != nil || count <= 0 {
			conn.Write([]byte("-ERR bad array count\r\n"))
			continue
		}

		args := []string{}
		for i := 0; i < count; i++ {
			bulkLine, err := reader.ReadString('\n')
			if err != nil || len(bulkLine) == 0 || bulkLine[0] != '$' {
				conn.Write([]byte("-ERR expected bulk string\r\n"))
				return
			}

			bulkLen, err := strconv.Atoi(strings.TrimSpace(bulkLine[1:]))
			if err != nil || bulkLen < 0 {
				conn.Write([]byte("-ERR invalid bulk length\r\n"))
				return
			}

			data := make([]byte, bulkLen+2)
			_, err = io.ReadFull(reader, data)
			if err != nil {
				conn.Write([]byte("-ERR reading bulk string\r\n"))
				return
			}
			args = append(args, string(data[:bulkLen]))
		}

		if len(args) == 0 {
			conn.Write([]byte("-ERR empty command\r\n"))
			continue
		}

		cmd := strings.ToUpper(args[0])

		if cmd == "PING" {
			if len(args) == 1 {
				conn.Write([]byte("+PONG\r\n"))
			} else {
				conn.Write([]byte("$" + strconv.Itoa(len(args[1])) + "\r\n" + args[1] + "\r\n"))
			}
		} else if cmd == "SET" {
			if len(args) != 3 {
				conn.Write([]byte("-ERR SET needs 2 args\r\n"))
				continue
			}
			store.Set(args[1], args[2])
			conn.Write([]byte("+OK\r\n"))
		} else if cmd == "GET" {
			if len(args) != 2 {
				conn.Write([]byte("-ERR GET needs 1 arg\r\n"))
				continue
			}
			val, found := store.Get(args[1])
			if found {
				conn.Write([]byte("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}
		} else if cmd == "DEL" {
			if len(args) != 2 {
				conn.Write([]byte("-ERR DEL needs 1 arg\r\n"))
				continue
			}
			if store.Delete(args[1]) {
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}
		} else {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func main() {
	store := NewStore()
	ln, err := net.Listen("tcp", ":"+PORT)
	if err != nil {
		log.Fatal("Error starting server:", err)
	}
	defer ln.Close()

	fmt.Println("Listening on port", PORT)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}
		go handleClient(conn, store)
	}
}
