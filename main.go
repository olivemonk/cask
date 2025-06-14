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
	"time"
)

const PORT = "6380"

type Entry struct {
	value string
	expiresAt time.Time
	hasExpiry bool
}

type Store struct {
	lock sync.Mutex
	kv map[string]Entry
}

func NewStore() *Store {
	store := &Store{
		kv: make(map[string]Entry),
	}
	return store
}

func (s *Store) Set(key string, value string, ttlSeconds int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	entry := Entry{value: value}
	if ttlSeconds > 0  {
		entry.hasExpiry = true
		entry.expiresAt = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	}
	s.kv[key] = entry
}

func (s *Store) Get(key string) (string, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	entry, found := s.kv[key]

	if !found {
		return "", false
	}
	if entry.hasExpiry && time.Now().After(entry.expiresAt) {
		delete(s.kv, key)
		return "", false
	}
	return entry.value, true
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
	log.Printf("Client connected: %s", conn.RemoteAddr())
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from client:", err)
			}
			break
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 || !strings.HasPrefix(line, "*") {
			conn.Write([]byte("-ERR expected array input\r\n"))
			continue
		}

		count, err := strconv.Atoi(line[1:])
		if err != nil || count <= 0 {
			conn.Write([]byte("-ERR invalide argument count\r\n"))
			continue
		}

		args := make([]string, 0, count)
		for i := 0; i < count; i++ {
			bulkLine, err := reader.ReadString('\n')
			if err != nil || !strings.HasPrefix(bulkLine, "$") {
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

		switch cmd {
		case "PING":
			if len(args) == 1 {
				conn.Write([]byte("+PONG\r\n"))
			} else if len(args) == 2 {
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
				conn.Write([]byte(resp))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for PING\r\n"))
			}

		case "SET":
			if len(args) < 3 || len(args) > 5 {
				conn.Write([]byte("-ERR SET requires 2 arguments, optionally with EX <seconds>\r\n"))
				continue
			}
			ttl := 0
			if len(args) == 5 && strings.ToUpper(args[3]) == "EX" {
				ttl, err = strconv.Atoi(args[4])
				if err != nil || ttl < 0 {
					conn.Write([]byte("-ERR invalid TTL\r\n"))
					continue
				}
			}
			store.Set(args[1], args[2], ttl)
			conn.Write([]byte("+OK\r\n"))

		case "GET":
			if len(args) != 2 {
				conn.Write([]byte("-ERR GET needs 1 argument\r\n"))
				continue
			}
			val, ok := store.Get(args[1])
			if ok {
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
				conn.Write([]byte(resp))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}

		case "DEL":
			if len(args) != 2 {
				conn.Write([]byte("-ERR DEL needs 1 argument\r\n"))
				continue
			}
			deleted := store.Delete(args[1])
			if deleted {
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}

		default:
			conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])))
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
