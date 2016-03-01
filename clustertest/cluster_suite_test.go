// +build cluster

package clustertest

import (
	"flag"
	"fmt"
	"os"
	"testing"
)

// NOTE: clst is not threadsafe. It is assumed (for the moment) that it
// is setup in TestMain, prior to access from multiple goroutines in
// tests, and that goroutines only concurrenly read clst, and call
// methods defined on the Cluster interface, rather than methods defined
// on the underlying type.
var clst Cluster

func TestMain(m *testing.M) {
	binPath := flag.String("bin", "", "Location of influxd binary")
	hybridN := flag.Int("hybrid", 3, "Number of hybrid nodes in the cluster")
	metaN := flag.Int("meta", 0, "Number of consensus nodes in the cluster")
	dataN := flag.Int("data", 0, "Number of data nodes in the cluster")
	flag.Parse()

	var err error
	// Let's create a cluster of 3 hybrid nodes. Here we could switch
	// this out for a remote cluster.
	if clst, err = newlocal(*hybridN, *metaN, *dataN, *binPath); err != nil {
		panic(err)
	}

	if err := clst.Start(); err != nil {
		clst.Stop()
		panic(err)
	}

	code := m.Run()
	clst.Stop()
	os.Exit(code)
}

func checkPanic(t *testing.T) {
	if r := recover(); r != nil {
		t.Logf("Panic in test detected: %v", r)
		clst.Stop()
	}
}

// TestShowDropDatabase tests that a database is available on all data
// nodes, and that when it's dropped, it's dropped from all data nodes.
func TestShowDropDatabase(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)

	dbName, err := clst.NewDatabase()
	if err != nil {
		t.Fatal(err)
	}

	// Verify that the database exists on all nodes in the cluster.
	for resp := range clst.QueryAll("SHOW DATABASES", "") {
		result, err := databases(resp.result)
		if err != nil {
			t.Fatal(err)
		}

		if !result.HasDatabase(dbName) {
			t.Fatalf("Node %d does not have database %s", resp.nodeID, dbName)
		}
		t.Logf("Node %d has database %s", resp.nodeID, dbName)
	}

	// When the database is dropped, it should be dropped on all nodes.
	qr := clst.QueryAny(fmat("DROP DATABASE %q", dbName), "")
	if qr.err != nil {
		t.Fatalf("[node %d] %v", qr.nodeID, qr.err)
	}

	for resp := range clst.QueryAll("SHOW DATABASES", "") {
		result, err := databases(resp.result)
		if err != nil {
			t.Fatal(err)
		}

		if result.HasDatabase(dbName) {
			t.Errorf("Node %d still has database %s", resp.nodeID, dbName)
		}
	}
}

func TestShowDropMeasurements(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)
	t.Skip("Not implemented yet")
}

func TestShowDropSeries(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)
	t.Skip("Not implemented yet")
}

func TestShowTagKeys(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)
	t.Skip("Not implemented yet")
}

func TestShowTagValues(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)
	t.Skip("Not implemented yet")
}

func TestFieldKeys(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)
	t.Skip("Not implemented yet")
}

func fmat(f string, v ...interface{}) string {
	return fmt.Sprintf(f, v...)
}
