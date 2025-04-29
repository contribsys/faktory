package client

var (
	Name    = "Faktory"
	Version = "1.9.2"
)

// Structs for parsing the INFO response
type FaktoryState struct {
	Now           string         `json:"now"`
	ServerUtcTime string         `json:"server_utc_time"`
	Data          DataSnapshot   `json:"faktory"`
	Server        ServerSnapshot `json:"server"`
}

type DataSnapshot struct {
	Queues         map[string]uint64         `json:"queues"`
	Sets           map[string]uint64         `json:"sets"`
	Tasks          map[string]map[string]any `json:"tasks"` // deprecated
	TotalFailures  uint64                    `json:"total_failures"`
	TotalProcessed uint64                    `json:"total_processed"`
	TotalEnqueued  uint64                    `json:"total_enqueued"`
	TotalQueues    uint64                    `json:"total_queues"`
}

type ServerSnapshot struct {
	Description  string `json:"description"`
	Version      string `json:"faktory_version"`
	Uptime       uint64 `json:"uptime"`
	Connections  uint64 `json:"connections"`
	CommandCount uint64 `json:"command_count"`
	UsedMemoryMB uint64 `json:"used_memory_mb"`
}
