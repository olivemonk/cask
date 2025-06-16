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
	"path/filepath"

)

const serverPort = "6380"

type Entry struct {
	value     string
	expiresAt time.Time
	hasExpiry bool
}

type Store struct {
	mu   sync.Mutex
	data map[string]Entry
}

func NewStore() *Store {
	store := &Store{
		data: make(map[string]Entry),
	}
	go store.cleanupExpiredKeys()
	return store
}

func (s *Store) Set(key, value string, ttlSeconds int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := Entry{value: value}
	if ttlSeconds > 0 {
		entry.hasExpiry = true
		entry.expiresAt = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	}
	s.data[key] = entry
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.data[key]
	if !found {
		return "", false
	}
	if entry.hasExpiry && time.Now().After(entry.expiresAt) {
		delete(s.data, key)
		return "", false
	}
	return entry.value, true
}

func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, found := s.data[key]
	if found {
		delete(s.data, key)
		return true
	}
	return false
}

func (s *Store) Exists(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.data[key]
	if !found || (entry.hasExpiry && time.Now().After(entry.expiresAt)) {
		if found {
			delete(s.data, key)
		}
		return false
	}
	return true
}

func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.data[key]
	if !found {
		return false
	}
	entry.hasExpiry = false
	s.data[key] = entry
	return true
}

func (s *Store) FlushAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]Entry)
}

func (s *Store) Keys(pattern string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	matching := []string{}
	for k, v := range s.data {
		if v.hasExpiry && time.Now().After(v.expiresAt) {
			delete(s.data, k)
			continue
		}
		match, _ := filepath.Match(pattern, k)
		if match {
			matching = append(matching, k)
		}
	}
	return matching
}

func (s *Store) Rename(oldKey, newKey string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.data[oldKey]
	if !found {
		return false
	}
	delete(s.data, oldKey)
	s.data[newKey] = entry
	return true
}

func (s *Store) TTL(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.data[key]
	if !found {
		return -2
	}
	if !entry.hasExpiry {
		return -1
	}
	ttl := int(time.Until(entry.expiresAt).Seconds())
	if ttl < 0 {
		delete(s.data, key)
		return -2
	}
	return ttl
}

func (s *Store) Expire(key string, seconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.data[key]
	if !found {
		return false
	}
	entry.hasExpiry = true
	entry.expiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
	s.data[key] = entry
	return true
}

func (s *Store) cleanupExpiredKeys() {
	for {
		time.Sleep(1 * time.Second)
		s.mu.Lock()
		now := time.Now()
		for k, v := range s.data {
			if v.hasExpiry && now.After(v.expiresAt) {
				delete(s.data, k)
			}
		}
		s.mu.Unlock()
	}
}

func handleConnection(conn net.Conn, store *Store) {
	defer conn.Close()
	log.Printf("Client connected: %s", conn.RemoteAddr())
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Println("Error reading from client:", err)
			}
			break
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 || !strings.HasPrefix(line, "*") {
			conn.Write([]byte("-ERR expected array input\r\n"))
			continue
		}

		numArgs, err := strconv.Atoi(line[1:])
		if err != nil || numArgs <= 0 {
			conn.Write([]byte("-ERR invalid argument count\r\n"))
			continue
		}

		args := make([]string, 0, numArgs)
		for i := 0; i < numArgs; i++ {
			bulkLenLine, err := reader.ReadString('\n')
			if err != nil || !strings.HasPrefix(bulkLenLine, "$") {
				conn.Write([]byte("-ERR expected bulk string\r\n"))
				return
			}

			bulkLen, err := strconv.Atoi(strings.TrimSpace(bulkLenLine[1:]))
			if err != nil || bulkLen < 0 {
				conn.Write([]byte("-ERR invalid bulk length\r\n"))
				return
			}

			bulk := make([]byte, bulkLen+2)
			_, err = io.ReadFull(reader, bulk)
			if err != nil {
				conn.Write([]byte("-ERR could not read bulk string\r\n"))
				return
			}

			args = append(args, string(bulk[:bulkLen]))
		}

		if len(args) == 0 {
			conn.Write([]byte("-ERR no command received\r\n"))
			continue
		}

		command := strings.ToUpper(args[0])

		switch command {
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
			if len(args) >= 4 && strings.ToUpper(args[3]) == "EX" {
				if len(args) != 5 {
					conn.Write([]byte("-ERR wrong number of arguments for SET with EX\r\n"))
					continue
				}
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
			deleted := store.Del(args[1])
			if deleted {
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}
		case "EXISTS":
			if len(args) != 2 {
				conn.Write([]byte("-ERR EXISTS needs 1 argument\r\n"))
				continue
			}
			if store.Exists(args[1]) {
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}
		case "PERSIST":
			if len(args) != 2 {
				conn.Write([]byte("-ERR PERSIST needs 1 argument\r\n"))
				continue
			}
			if store.Persist(args[1]) {
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}
		case "FLUSHALL":
			store.FlushAll()
			conn.Write([]byte("+OK\r\n"))
		case "KEYS":
			if len(args) != 2 {
				conn.Write([]byte("-ERR KEYS needs 1 argument\r\n"))
				continue
			}
			keys := store.Keys(args[1])
			var b strings.Builder
			b.WriteString(fmt.Sprintf("*%d\r\n", len(keys)))
			for _, key := range keys {
				b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))
			}
			conn.Write([]byte(b.String()))
		case "RENAME":
			if len(args) != 3 {
				conn.Write([]byte("-ERR RENAME needs 2 arguments\r\n"))
				continue
			}
			if !store.Exists(args[1]) {
				conn.Write([]byte("-ERR no such key\r\n"))
				continue
			}
			store.Rename(args[1], args[2])
			conn.Write([]byte("+OK\r\n"))
		case "TTL":
			if len(args) != 2 {
				conn.Write([]byte("-ERR TTL needs 1 argument\r\n"))
				continue
			}
			ttl := store.TTL(args[1])
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", ttl)))
		case "EXPIRE":
			if len(args) != 3 {
				conn.Write([]byte("-ERR EXPIRE needs 2 arguments\r\n"))
				continue
			}
			seconds, err := strconv.Atoi(args[2])
			if err != nil || seconds < 0 {
				conn.Write([]byte("-ERR invalid TTL\r\n"))
				continue
			}
			if store.Expire(args[1], seconds) {
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
	ln, err := net.Listen("tcp", ":"+serverPort)
	if err != nil {
		log.Fatal("Error starting server:", err)
	}
	defer ln.Close()

	fmt.Println("CASK server started on port:", serverPort)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn, store)
	}
}



