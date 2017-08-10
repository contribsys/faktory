package faktory

var (
	Name        = "Faktory"
	License     = "GPLv3"
	Licensing   = "Licensed under the GNU Public License 3.0"
	Version     = "0.0.1"
	Environment = "development"

	EventHandlers = make([]func(*Server), 0)
)

func OnStart(x func(*Server)) {
	EventHandlers = append(EventHandlers, x)
}
