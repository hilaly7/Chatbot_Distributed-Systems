// Chat client: connects to the server over net/rpc and polls for messages.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatapp/common"
)

type client struct {
	rpc      *rpc.Client
	name     string
	printMu  sync.Mutex // keeps terminal output from interleaving
	stopOnce sync.Once
	stop     chan struct{}
}

func (c *client) call(method string, args, reply any) error {
	return c.rpc.Call(common.ServiceName+"."+method, args, reply)
}

// out prints a line above the prompt and redraws the prompt.
func (c *client) out(format string, a ...any) {
	c.printMu.Lock()
	defer c.printMu.Unlock()
	fmt.Printf("\r\033[K"+format+"\n> ", a...)
}

func (c *client) shutdown() {
	c.stopOnce.Do(func() { close(c.stop) })
}

// pollLoop periodically asks the server for pending messages.
func (c *client) pollLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			var r common.PollReply
			if err := c.call("Poll", &common.PollArgs{Username: c.name}, &r); err != nil {
				select {
				case <-c.stop: // we are already leaving
				default:
					c.out("!! lost connection to server: %v", err)
					c.shutdown()
				}
				return
			}
			for _, m := range r.Messages {
				ts := m.Time.Format("15:04:05")
				if m.IsSystem {
					c.out("[%s] * %s", ts, m.Text)
				} else {
					c.out("[%s] %s: %s", ts, m.From, m.Text)
				}
			}
		}
	}
}

func main() {
	server := flag.String("server", "localhost:1234", "server address host:port")
	name := flag.String("name", "", "your username (prompted if empty)")
	interval := flag.Duration("poll", 500*time.Millisecond, "poll interval")
	flag.Parse()

	in := bufio.NewReader(os.Stdin)
	if *name == "" {
		fmt.Print("Username: ")
		line, _ := in.ReadString('\n')
		*name = strings.TrimSpace(line)
	}

	rc, err := rpc.Dial("tcp", *server)
	if err != nil {
		log.Fatalf("cannot connect to %s: %v", *server, err)
	}
	c := &client{rpc: rc, name: *name, stop: make(chan struct{})}

	var jr common.JoinReply
	if err := c.call("Join", &common.JoinArgs{Username: *name}, &jr); err != nil {
		fmt.Println("join failed:", err)
		rc.Close()
		os.Exit(1)
	}
	fmt.Println(jr.Message)
	fmt.Println("Commands: /users  /quit   (anything else is sent as a message)")

	go c.pollLoop(*interval)

	// Ctrl+C / SIGTERM -> leave cleanly.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() { <-sig; c.shutdown() }()

	// Read stdin in its own goroutine so we can also react to c.stop.
	lines := make(chan string)
	go func() {
		defer close(lines)
		for {
			fmt.Print("> ")
			l, err := in.ReadString('\n')
			if err != nil {
				return // EOF (Ctrl+D / closed pipe)
			}
			lines <- strings.TrimSpace(l)
		}
	}()

loop:
	for {
		select {
		case <-c.stop:
			break loop
		case l, ok := <-lines:
			if !ok {
				break loop
			}
			switch {
			case l == "":
			case l == "/quit":
				break loop
			case l == "/users":
				var r common.ListReply
				if err := c.call("ListUsers", &common.ListArgs{Username: c.name}, &r); err != nil {
					c.out("!! %v", err)
				} else {
					c.out("online (%d): %s", len(r.Users), strings.Join(r.Users, ", "))
				}
			default:
				if err := c.call("Send", &common.SendArgs{Username: c.name, Text: l}, &common.SendReply{}); err != nil {
					c.out("!! send failed: %v", err)
				}
			}
		}
	}

	c.shutdown()
	_ = c.call("Leave", &common.LeaveArgs{Username: c.name}, &common.LeaveReply{})
	rc.Close()
	fmt.Println("\nbye!")
}
