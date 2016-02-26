package clustertest

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/cmd/influxd/run"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
)

func newConfig(basePth string, i int, nt string) *run.Config {
	if i <= 0 {
		panic(fmt.Sprintf("invalid node number %d; must be >= 1", i))
	}

	c := run.NewConfig()
	portDelta := i * portJump

	// General configuration.
	c.ReportingDisabled = true
	c.Meta.Enabled, c.Data.Enabled = false, false // enable these as we need them
	c.HintedHandoff.Enabled = false
	c.BindAddress = mustShiftPort(meta.DefaultRaftBindAddress, portDelta)

	switch nt {
	case "meta":
		configureMetaNode(basePth, portDelta, c)
	case "data":
		configureDataNode(basePth, portDelta, c)
	case "hybrid":
		configureMetaNode(basePth, portDelta, c)
		configureDataNode(basePth, portDelta, c)
	default:
		panic(fmt.Sprintf("unsupported node type %q", nt))
	}
	return c
}

// configureMetaNode configures the Meta node specific part of a node
// configuration file.
func configureMetaNode(basePth string, portDelta int, c *run.Config) {
	c.Meta.Enabled = true
	// Configure directories.
	c.Meta.Dir = filepath.Join(basePth, ".influxdb/meta")

	// Configure ports.
	c.Meta.BindAddress = mustShiftPort(meta.DefaultRaftBindAddress, portDelta)
	c.Meta.HTTPBindAddress = mustShiftPort(meta.DefaultHTTPBindAddress, portDelta)
}

// configureDataNode configures the Meta node specific part of a node
// configuration file.
func configureDataNode(basePth string, portDelta int, c *run.Config) {
	c.Data.Enabled = true
	// Configure directories.
	c.Data.Dir = filepath.Join(basePth, ".influxdb/data")
	c.Data.WALDir = filepath.Join(basePth, ".influxdb/wal")

	// Configure ports.
	c.HTTPD.BindAddress = mustShiftPort(httpd.DefaultBindAddress, portDelta)
	c.Meta.BindAddress = mustShiftPort(meta.DefaultRaftBindAddress, portDelta)
	c.Meta.HTTPBindAddress = mustShiftPort(meta.DefaultHTTPBindAddress, portDelta)
}

// shiftPort adjusts the port element of an input host:port or :port
// address by delta.
// For example: shiftPort(":8080", 300) returns ":8380".
func shiftPort(in string, delta int) (string, error) {
	parts := strings.Split(in, ":")
	if len(parts) > 2 {
		return "", fmt.Errorf("cannot parse %q", in)
	}

	port, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return "", err
	}

	if len(parts) == 1 {
		return fmt.Sprintf("%d", port+delta), nil
	}
	return fmt.Sprintf("%s:%d", parts[0], port+delta), nil
}

func mustShiftPort(in string, delta int) string {
	port, err := shiftPort(in, delta)
	if err != nil {
		panic(err)
	}
	return port
}

// ptoi extracts the port number from a host:port or :port string.
func ptoi(s string) (int, error) {
	var (
		parts = strings.Split(s, ":")
		i     = len(parts) - 1
	)

	if i < 0 {
		return 0, fmt.Errorf("cannot extract port from %q", s)
	}

	return strconv.Atoi(parts[i])
}

func mustptoi(s string) int {
	port, err := ptoi(s)
	if err != nil {
		panic(err)
	}
	return port
}
