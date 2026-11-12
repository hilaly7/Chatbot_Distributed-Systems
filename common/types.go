// Package common holds the types shared by the chat server and client.
package common

import "time"

// ServiceName is the name the chat service is registered under in net/rpc.
const ServiceName = "ChatService"

// Message is a single chat message or a system notification.
type Message struct {
	From     string    // sender username ("*server*" for notifications)
	Text     string    // message body
	Time     time.Time // when the server received it
	IsSystem bool      // true for join/leave notifications
}

// JoinArgs / JoinReply: register a unique username.
type JoinArgs struct{ Username string }
type JoinReply struct{ Message string }

// LeaveArgs / LeaveReply: disconnect.
type LeaveArgs struct{ Username string }
type LeaveReply struct{}

// SendArgs / SendReply: broadcast a message to everyone except the sender.
type SendArgs struct {
	Username string
	Text     string
}
type SendReply struct{}

// PollArgs / PollReply: fetch (and clear) the pending messages for a user.
type PollArgs struct{ Username string }
type PollReply struct{ Messages []Message }

// ListArgs / ListReply: list connected usernames.
type ListArgs struct{ Username string }
type ListReply struct{ Users []string }
