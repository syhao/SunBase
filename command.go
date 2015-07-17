package main

import (
	"encoding/json"
)

const (
	OpPut = 0x00
	OpGet = 0x01
)

type Command struct {
	Op uint8
	K  string
	V  string
}

func NewCommand() *Command {
	return new(Command)
}

func (c *Command) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func (c *Command) UnMarshal(data []byte) error {
	return json.Unmarshal(data, c)
}
