package store

import (
	"context"
	"fmt"
	"strings"
	"time"
	_ "time/tzdata"
)

// FormatMessage renders a single message in the legacy textual format expected
// by downstream Claude clients.
func (s *Store) FormatMessage(ctx context.Context, m Message, showChatInfo bool) string {
	var b strings.Builder

	b.WriteString("[")
	b.WriteString(m.Timestamp.Format("2006-01-02 15:04:05"))
	b.WriteString("] ")

	if showChatInfo && m.ChatName != "" {
		b.WriteString("Chat: ")
		b.WriteString(m.ChatName)
		b.WriteString(" ")
	}

	b.WriteString("ID: ")
	b.WriteString(m.ID)
	b.WriteString(" TZ: ")
	b.WriteString(m.Timestamp.Format("-07:00"))
	b.WriteString(" Sender: ")
	sender := m.Sender
	if strings.HasSuffix(m.ChatJID, "@g.us") && (sender == m.ChatJID || sender == jidUser(m.ChatJID)) {
		sender = ""
	}
	if sender == "" {
		b.WriteString("unknown")
	} else {
		b.WriteString(sender)
	}
	b.WriteString(" ")

	contentPrefix := ""
	if m.MediaType != "" {
		contentPrefix = "[" + m.MediaType + " - Message ID: " + m.ID + " - Chat JID: " + m.ChatJID + "] "
	}

	var senderName string
	if m.IsFromMe {
		senderName = "Me"
	} else {
		senderName = s.GetSenderName(ctx, sender)
	}

	b.WriteString("From: ")
	b.WriteString(senderName)
	b.WriteString(": ")
	b.WriteString(contentPrefix)
	b.WriteString(visibleMessageText(m, m.Content))
	b.WriteString("\n")

	return b.String()
}

// FormatMessagesList renders a slice of messages. Returns the literal string
// "No messages to display." when the slice is empty.
func (s *Store) FormatMessagesList(ctx context.Context, msgs []Message, showChatInfo bool) string {
	if len(msgs) == 0 {
		return "No messages to display."
	}
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(s.FormatMessage(ctx, m, showChatInfo))
	}
	return b.String()
}

// ChatDisplayName uses the authoritative address book for direct chats.
// A cached name may have been inferred incorrectly from an outgoing sender.
func (s *Store) ChatDisplayName(jid, cached string) string {
	if !strings.HasSuffix(jid, "@g.us") {
		if name := s.ContactName(jid); name != "" {
			return name
		}
	}
	if cached != "" {
		return cached
	}
	return jidUser(jid)
}

// FormatMessagesCompact keeps transport IDs out of normal reading. Detailed
// output remains available for quoting, media downloads and diagnostics.
func (s *Store) FormatMessagesCompact(ctx context.Context, msgs []Message, timezone string) (string, error) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return "", fmt.Errorf("unknown timezone %q", timezone)
	}
	if len(msgs) == 0 {
		return "No cached messages matched.", nil
	}
	var b strings.Builder
	lastChat, lastDate, lastOffset := "", "", ""
	for _, m := range msgs {
		ts := m.Timestamp.In(loc)
		date := ts.Format("2006-01-02")
		offset := ts.Format("-07:00")
		if m.ChatJID != lastChat || date != lastDate || offset != lastOffset {
			name := strings.NewReplacer("\n", " ", "\r", " ").Replace(s.ChatDisplayName(m.ChatJID, m.ChatName))
			fmt.Fprintf(&b, "%s | %s | %s (UTC%s) | newest first\n", name, date, timezone, offset)
			lastChat, lastDate, lastOffset = m.ChatJID, date, offset
		}
		sender := m.Sender
		if strings.HasSuffix(m.ChatJID, "@g.us") && (sender == m.ChatJID || sender == jidUser(m.ChatJID)) {
			sender = ""
		}
		author := s.GetSenderName(ctx, sender)
		if m.IsFromMe {
			author = "You"
		}
		author = strings.NewReplacer("\n", " ", "\r", " ").Replace(author)
		fmt.Fprintf(&b, "%s %s: ", ts.Format("15:04"), author)
		if m.MediaType != "" {
			fmt.Fprintf(&b, "[%s] ", m.MediaType)
		}
		text, _, mentionsMe := s.RenderMentions(ctx, m, false)
		if mentionsMe {
			b.WriteString("[mentions you] ")
		}
		b.WriteString(strings.ReplaceAll(visibleMessageText(m, text), "\n", "\n  "))
		b.WriteByte('\n')
	}
	return b.String(), nil
}

func visibleMessageText(m Message, text string) string {
	if m.ContentStatus == "unknown" {
		return "[caption not synchronized]"
	}
	if m.ContentStatus == "known" && text == "" && (m.MediaType == "image" || m.MediaType == "video" || m.MediaType == "document") {
		return "[no caption]"
	}
	return text
}
func (s *Store) FormatMessagesDetailed(ctx context.Context, messages []Message, showChat bool, timezone string) (string, error) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return "", err
	}
	localized := append([]Message(nil), messages...)
	for i := range localized {
		localized[i].Timestamp = localized[i].Timestamp.In(loc)
	}
	return "Timezone: " + timezone + "\n" + s.FormatMessagesList(ctx, localized, showChat), nil
}
