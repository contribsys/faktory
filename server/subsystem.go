package server

type Subsystem interface {
	Name() string

	// Called when the server is configured but before it starts accepting client connections.
	Start(*Server) error

	// Called every time Faktory reloads the global config for the Server.
	// Each subsystem is responsible for diffing its own config and making
	// necessary changes.
	Reload(*Server) error

	// Shutdown is signaled by the Server.Stopper() channel.  Subsystems should
	// select on it to be notified when the server is shutting down.
}

// register a global handler to be called when the Server instance
// has finished booting but before it starts listening.
func (s *Server) Register(x Subsystem) {
	s.Subsystems = append(s.Subsystems, x)
}
