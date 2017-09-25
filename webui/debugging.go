package webui

import "os/exec"

func df_h() string {
	cmd := exec.Command("df", "-h")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return err.Error()
	}
	return string(out)
}
