package client

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/contribsys/faktory/util"
)

type JobTrack struct {
	Jid         string `json:"jid"`
	Percent     int    `json:"percent,omitempty"`
	Description string `json:"desc,omitempty"`
	State       string `json:"state"`
	UpdatedAt   string `json:"updated_at"`
}

func (c *Client) TrackGet(jid string) (*JobTrack, error) {
	err := c.writeLine(c.wtr, "TRACK GET", []byte(jid))
	if err != nil {
		return nil, err
	}

	data, err := c.readResponse(c.rdr)
	if err != nil {
		return nil, err
	}

	var trck JobTrack
	err = util.JsonUnmarshal(data, &trck)
	if err != nil {
		return nil, err
	}

	return &trck, nil
}

type setJobTrack struct {
	Jid          string `json:"jid"`
	Percent      int    `json:"percent,omitempty"`
	Description  string `json:"desc,omitempty"`
	ReserveUntil string `json:"reserve_until,omitempty"`
}

func (c *Client) TrackSet(jid string, percent int, desc string, reserveUntil *time.Time) error {
	if jid == "" {
		return fmt.Errorf("Job Track missing JID")
	}

	tset := setJobTrack{
		Jid:         jid,
		Description: desc,
		Percent:     percent,
	}
	if reserveUntil != nil && time.Now().Before(*reserveUntil) {
		tset.ReserveUntil = util.Thens(*reserveUntil)
	}
	return c.trackSet(&tset)
}

func (c *Client) trackSet(tset *setJobTrack) error {
	data, err := json.Marshal(tset)
	if err != nil {
		return err
	}

	err = c.writeLine(c.wtr, "TRACK SET", data)
	if err != nil {
		return err
	}

	return c.ok(c.rdr)
}
