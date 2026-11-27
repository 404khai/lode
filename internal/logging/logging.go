// Package logging builds the process-wide slog logger.
package logging

import (
	"io"
	"log/slog"
	"strings"
)

// New returns a logger writing to w in the given format ("json" or "text").
func New(w io.Writer, level, format, component string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if strings.EqualFold(format, "text") {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h).With("component", component)
}
