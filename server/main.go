// Chat server: manages users and distributes messages over Go net/rpc.
// Communication approach: POLLING (clients call ChatService.Poll periodically).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatapp/common"
)

const maxQueue = 1000 // max pending messages per user (protects memory)

type user struct {
	queue    []common.Message
	lastSeen time.Time
}

// ChatService is the RPC receiver. All state is guarded by mu.
type ChatService struct {
	mu    sync.Mutex
	users map[string]*user
}

func NewChatService() *ChatService {
	return &ChatService{users: make(map[string]*user)}
}

func validName(n string) error {
	n = strings.TrimSpace(n)
	switch {
	case n == "":
		return errors.New("username cannot be empty")
	case len(n) > 20:
		return errors.New("username too long (max 20 chars)")
	case strings.ContainsAny(n, " \t\r\n"):
		return errors.New("username cannot contain spaces")
	case n == "*server*":
		return errors.New("username is reserved")
	}
	return nil
}

// enqueueLocked appends msg to every user's queue except `except`. Caller holds mu.
func (s *ChatService) enqueueLocked(msg common.Message, except string) {
	for name, u := range s.users {
		if name == except {
			continue
		}
		if len(u.queue) >= maxQueue {
			u.queue = u.queue[1:] // drop oldest
		}
		u.queue = append(u.queue, msg)
	}
}

func system(text string) common.Message {
	return common.Message{From: "*server*", Text: text, Time: time.Now(), IsSystem: true}
}

// Join registers a username; duplicates are rejected.
func (s *ChatService) Join(args *common.JoinArgs, reply *common.JoinReply) error {
	name := strings.TrimSpace(args.Username)
	if err := validName(name); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[name]; ok {
		return fmt.Errorf("username %q is already taken", name)
	}
	s.users[name] = &user{lastSeen: time.Now()}
	s.enqueueLocked(system(name+" joined the chat"), name)
	reply.Message = fmt.Sprintf("welcome %s! %d user(s) online", name, len(s.users))
	log.Printf("JOIN  %s (%d online)", name, len(s.users))
	return nil
}

// removeLocked removes a user and notifies the rest. Caller holds mu.
func (s *ChatService) removeLocked(name, why string) bool {
	if _, ok := s.users[name]; !ok {
		return false
	}
	delete(s.users, name)
	s.enqueueLocked(system(name+" "+why), name)
	log.Printf("LEAVE %s (%s) (%d online)", name, why, len(s.users))
	return true
}

// Leave disconnects a user gracefully.
func (s *ChatService) Leave(args *common.LeaveArgs, _ *common.LeaveReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.removeLocked(args.Username, "left the chat") {
		return errors.New("not connected")
	}
	return nil
}

// Send broadcasts to everyone except the sender.
func (s *ChatService) Send(args *common.SendArgs, _ *common.SendReply) error {
	text := strings.TrimSpace(args.Text)
	if text == "" {
		return errors.New("empty message")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[args.Username]
	if !ok {
		return errors.New("not connected: join first")
	}
	u.lastSeen = time.Now()
	s.enqueueLocked(common.Message{From: args.Username, Text: text, Time: time.Now()}, args.Username)
	return nil
}

// Poll returns and clears the pending messages. It doubles as a heartbeat.
func (s *ChatService) Poll(args *common.PollArgs, reply *common.PollReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[args.Username]
	if !ok {
		return errors.New("not connected")
	}
	u.lastSeen = time.Now()
	reply.Messages = u.queue
	u.queue = nil
	return nil
}

// ListUsers returns the sorted list of connected usernames.
func (s *ChatService) ListUsers(args *common.ListArgs, reply *common.ListReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u, ok := s.users[args.Username]; ok {
		u.lastSeen = time.Now()
	} else {
		return errors.New("not connected")
	}
	for n := range s.users {
		reply.Users = append(reply.Users, n)
	}
	sort.Strings(reply.Users)
	return nil
}

// reaper removes users that stopped polling (crash, network loss, kill -9).
func (s *ChatService) reaper(ctx context.Context, timeout time.Duration) {
	t := time.NewTicker(timeout / 4)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.mu.Lock()
			for name, u := range s.users {
				if time.Since(u.lastSeen) > timeout {
					s.removeLocked(name, "disconnected unexpectedly (timeout)")
				}
			}
			s.mu.Unlock()
		}
	}
}

func main() {
	addr := flag.String("addr", ":1234", "listen address")
	timeout := flag.Duration("timeout", 6*time.Second, "drop clients silent for this long")
	flag.Parse()

	svc := NewChatService()
	srv := rpc.NewServer()
	if err := srv.RegisterName(common.ServiceName, svc); err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("chat server listening on %s", ln.Addr())

	ctx, cancel := context.WithCancel(context.Background())
	go svc.reaper(ctx, *timeout)

	var wg sync.WaitGroup
	var connMu sync.Mutex
	conns := map[net.Conn]struct{}{}

	// Accept loop: one goroutine per client connection.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return // listener closed -> shutting down
			}
			connMu.Lock()
			conns[c] = struct{}{}
			connMu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				srv.ServeConn(c)
				connMu.Lock()
				delete(conns, c)
				connMu.Unlock()
			}()
		}
	}()

	// Clean shutdown on Ctrl+C / SIGTERM.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("shutting down...")
	cancel()
	ln.Close()
	<-done
	connMu.Lock()
	for c := range conns {
		c.Close()
	}
	connMu.Unlock()
	wg.Wait()
	log.Println("server stopped cleanly")
}
