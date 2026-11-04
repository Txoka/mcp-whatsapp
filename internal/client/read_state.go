package client

import (
	"context"
	"go.mau.fi/whatsmeow/appstate"
	"time"
)

// Refresh only our local copy of read markers. FetchAppState does not send
// read receipts or mark conversations read. A full snapshot is requested once
// for stores created before unread tracking existed; subsequent connects fetch
// incremental state through the library's ordinary synchronization.
func (c *Client) refreshReadState() {
	var done string
	c.store.DB().QueryRow("SELECT value FROM local_metadata WHERE key='read_state_bootstrap'").Scan(&done)
	if done == "done" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.wa.FetchAppState(ctx, appstate.WAPatchRegularLow, true, false); err != nil {
		c.log.Warnf("Read-state bootstrap unavailable: %v", err)
		return
	}
	if _, err := c.store.DB().Exec("INSERT OR REPLACE INTO local_metadata(key,value) VALUES('read_state_bootstrap','done')"); err != nil {
		c.log.Warnf("Read-state bootstrap flag failed: %v", err)
	}
}
