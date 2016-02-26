package clustertest

import (
	"flag"
	"fmt"
	"os"
	"testing"
)

// clst is not threadsafe. It is assumed (for the moment) that it is
// setup in TestMain, prior to access from multiple goroutines in tests,
// and that goroutines only concurrenly read from the cluster.
var clst Cluster

func TestMain(m *testing.M) {
	binPath := flag.String("bin", "", "Location of influxd binary")
	flag.Parse()

	var err error
	// Let's create a cluster of 3 hybrid nodes.
	if clst, err = newlocal(3, 0, 0, *binPath); err != nil {
		panic(err)
	}

	if err := clst.Start(); err != nil {
		clst.Close()
		panic(err)
	}

	code := m.Run()
	clst.Close()
	os.Exit(code)
}

// TestDropDatabase tests that dropping a database results in the
// database being dropped from all nodes in the cluster.
func TestDropDatabase(t *testing.T) {
	t.Parallel()

	dbName, err := clst.NewDatabase()
	if err != nil {
		t.Fatal(err)
	}

	// Verify that the database exists on all nodes in the cluster.
	for resp := range clst.RunAll("SHOW DATABASES", "") {
		result, err := databases(resp.result)
		if err != nil {
			t.Fatal(err)
		}

		if !result.HasDatabase(dbName) {
			t.Fatalf("Node %d does not have database %s", resp.nodeID, dbName)
		}
	}

	// When the database is dropped, it should be dropped on all nodes.
	qr := clst.RunAny(fmat("DROP DATABASE %q", dbName), "")
	if qr.err != nil {
		t.Fatalf("[node %d] %v", qr.nodeID, qr.err)
	}

	for resp := range clst.RunAll("SHOW DATABASES", "") {
		result, err := databases(resp.result)
		if err != nil {
			t.Fatal(err)
		}

		if result.HasDatabase(dbName) {
			t.Errorf("Node %d still has database %s", resp.nodeID, dbName)
		}
	}
}

func fmat(f string, v ...interface{}) string {
	return fmt.Sprintf(f, v...)
}
