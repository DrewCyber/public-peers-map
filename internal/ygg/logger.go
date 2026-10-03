package ygg

import (
	"fmt"
	"log/slog"
)

// Slogger adapts *slog.Logger to the core.Logger interface expected by
// yggdrasil-go.
type Slogger struct{ L *slog.Logger }

func (s Slogger) Printf(f string, a ...any) { s.L.Info(fmt.Sprintf(f, a...)) }
func (s Slogger) Println(a ...any)          { s.L.Info(fmt.Sprintln(a...)) }
func (s Slogger) Infof(f string, a ...any)  { s.L.Info(fmt.Sprintf(f, a...)) }
func (s Slogger) Infoln(a ...any)           { s.L.Info(fmt.Sprintln(a...)) }
func (s Slogger) Warnf(f string, a ...any)  { s.L.Warn(fmt.Sprintf(f, a...)) }
func (s Slogger) Warnln(a ...any)           { s.L.Warn(fmt.Sprintln(a...)) }
func (s Slogger) Errorf(f string, a ...any) { s.L.Error(fmt.Sprintf(f, a...)) }
func (s Slogger) Errorln(a ...any)          { s.L.Error(fmt.Sprintln(a...)) }
func (s Slogger) Debugf(f string, a ...any) { s.L.Debug(fmt.Sprintf(f, a...)) }
func (s Slogger) Debugln(a ...any)          { s.L.Debug(fmt.Sprintln(a...)) }
func (s Slogger) Traceln(a ...any)          { s.L.Debug(fmt.Sprintln(a...)) }
