// Package planner computes changes between completed and desired releases.
package planner

import (
	"sort"

	"github.com/carbocation/pgsc_mirror/internal/model"
)

type Kind string

const (
	Add       Kind = "add"
	Revise    Kind = "revise"
	Withdraw  Kind = "withdraw"
	Restore   Kind = "restore"
	Metadata  Kind = "metadata"
	Unchanged Kind = "unchanged"
)

type Change struct {
	PGSID string       `json:"pgs_id"`
	Kind  Kind         `json:"kind"`
	Old   *model.Entry `json:"old,omitempty"`
	New   *model.Entry `json:"new,omitempty"`
}

func Plan(previous, desired []model.Entry) []Change {
	old := make(map[string]model.Entry, len(previous))
	newEntries := make(map[string]model.Entry, len(desired))
	for _, e := range previous {
		old[e.PGSID] = e
	}
	for _, e := range desired {
		newEntries[e.PGSID] = e
	}
	ids := make(map[string]struct{}, len(old)+len(newEntries))
	for id := range old {
		ids[id] = struct{}{}
	}
	for id := range newEntries {
		ids[id] = struct{}{}
	}
	out := make([]Change, 0, len(ids))
	for id := range ids {
		o, had := old[id]
		n, has := newEntries[id]
		c := Change{PGSID: id}
		if had {
			oo := o
			c.Old = &oo
		}
		if has {
			nn := n
			c.New = &nn
		}
		switch {
		case !had && has:
			c.Kind = Add
		case had && !has:
			c.Kind = Withdraw
		case o.Status == model.StatusGone && n.Status == model.StatusReady:
			c.Kind = Restore
		case o.SourceMD5 != n.SourceMD5:
			c.Kind = Revise
		case o.Status != n.Status:
			c.Kind = Withdraw
		case o.License != n.License || o.SourceURL != n.SourceURL:
			c.Kind = Metadata
		default:
			c.Kind = Unchanged
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PGSID < out[j].PGSID })
	return out
}

func HasChanges(changes []Change) bool {
	for _, c := range changes {
		if c.Kind != Unchanged {
			return true
		}
	}
	return false
}
