package worq

import "github.com/mperham/worq/util"

var (
	Name        = "Worq"
	License     = "GPLv3"
	Licensing   = "Licensed under the GNU Public License 3.0"
	Version     = "0.0.1"
	Environment = "development"

	logger util.Logger
)

func InitializeLogger(level string) {
	logger = util.NewLogger(level, true)
}
