package client

import (
	"go.mau.fi/whatsmeow/types"
	"strings"
)

type groupSummary struct {
	ChatJID          string `json:"chat_jid"`
	Name             string `json:"name"`
	ParticipantCount int    `json:"participant_count"`
}
type participantView struct {
	Phone          *string `json:"phone"`
	ContactName    *string `json:"contact_name"`
	ProfileName    *string `json:"profile_name"`
	Reference      string  `json:"jid"`
	Role           string  `json:"role"`
	IsMe           bool    `json:"is_me,omitempty"`
	AnonymousLabel string  `json:"anonymous_label,omitempty"`
}
type groupView struct {
	Announce             bool              `json:"announce"`
	Locked               bool              `json:"locked"`
	JoinApprovalRequired bool              `json:"join_approval_required"`
	ChatJID              string            `json:"chat_jid"`
	Name                 string            `json:"name"`
	Topic                string            `json:"topic,omitempty"`
	ParticipantCount     int               `json:"participant_count"`
	Page                 int               `json:"page"`
	HasMore              bool              `json:"has_more"`
	Participants         []participantView `json:"participants"`
}

func knownString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func (c *Client) participantView(p types.GroupParticipant) participantView {
	pn := p.PhoneNumber.ToNonAD().String()
	lid := p.LID.ToNonAD().String()
	ref := p.JID.ToNonAD().String()
	phone := c.store.ResolveJIDToPhone(pn)
	if phone == "" {
		phone = c.store.ResolveJIDToPhone(ref)
	}
	if phone == "" {
		phone = c.store.ResolveJIDToPhone(lid)
	}
	contact, profile := c.store.ContactNames(pn, lid, ref)
	own := false
	if c.wa != nil && c.wa.Store != nil {
		ownIDs := map[string]bool{}
		if c.wa.Store.ID != nil {
			ownIDs[c.wa.Store.ID.ToNonAD().String()] = true
		}
		if !c.wa.Store.LID.IsEmpty() {
			ownIDs[c.wa.Store.LID.ToNonAD().String()] = true
		}
		own = ownIDs[pn] || ownIDs[lid] || ownIDs[ref]
		if own && profile == "" {
			profile = c.wa.Store.PushName
		}
	}
	role := "member"
	if p.IsAdmin {
		role = "admin"
	}
	if p.IsSuperAdmin {
		role = "superadmin"
	}
	if phone != "" && !strings.HasPrefix(phone, "+") {
		phone = "+" + phone
	}
	return participantView{Phone: knownString(phone), ContactName: knownString(contact), ProfileName: knownString(profile), Reference: ref, Role: role, IsMe: own, AnonymousLabel: p.DisplayName}
}
func (c *Client) groupView(info *types.GroupInfo, limit, page int) groupView {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	if page < 0 {
		page = 0
	}
	out := groupView{Announce: info.IsAnnounce, Locked: info.IsLocked, JoinApprovalRequired: info.IsJoinApprovalRequired, ChatJID: info.JID.String(), Name: info.Name, Topic: info.Topic, ParticipantCount: len(info.Participants), Page: page, Participants: []participantView{}}
	if page > len(info.Participants)/limit {
		return out
	}
	start := page * limit
	end := start + limit
	if end > len(info.Participants) {
		end = len(info.Participants)
	}
	out.HasMore = end < len(info.Participants)
	for _, p := range info.Participants[start:end] {
		out.Participants = append(out.Participants, c.participantView(p))
	}
	return out
}
