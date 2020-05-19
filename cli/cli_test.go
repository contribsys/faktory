package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadConfig(t *testing.T) {
	wd, _ := os.Getwd()
	config, _ := readConfig(filepath.Join(wd, "test-fixtures", "case-one"), "")

	schedule := config["cron"].([]map[string]interface{})
	jobOne := schedule[0]["job"].(map[string]interface{})

	if len(schedule) < 2 {
		t.Fatalf("Schedule did not include both items %v", schedule)
	}

	jobTwo := schedule[1]["job"].(map[string]interface{})

	got := jobOne["type"]
	if got != "OneJob" {
		t.Errorf("First job in schedule was %v, want OneJob", got)
	}

	got = jobTwo["type"]
	if got != "TwoJob" {
		t.Errorf("Second job in schedule was %v, want TwoJob", got)
	}
}
