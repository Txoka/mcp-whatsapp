package client

import "testing"

func TestHistorySender(t *testing.T) {
	cases := []struct {
		chat, own       string
		mine            bool
		key, info, want string
	}{
		{"999@g.us", "100", false, "123@s.whatsapp.net", "", "123"},
		{"999@g.us", "100", false, "", "456@lid", "456"},
		{"999@g.us", "100", false, "", "", ""},
		{"999@g.us", "100", false, "999@g.us", "", ""},
		{"999@g.us", "100", false, "999@g.us", "456@lid", "456"},
		{"999@g.us", "100", true, "", "", "100"},
		{"123@s.whatsapp.net", "100", false, "", "", "123"},
	}
	for _, c := range cases {
		_, got := historySender(c.chat, c.own, c.mine, c.key, c.info)
		if got != c.want {
			t.Errorf("%+v: got %s", c, got)
		}
	}
}
