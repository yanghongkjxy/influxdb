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
		panic(r)
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
	t.Logf("Verifying nodes have database database %s", dbName)
	for resp := range clst.QueryAll("SHOW DATABASES", "") {
		result, err := parseResult(ShowDatabases, resp.result)
		if err != nil {
			t.Fatal(err)
		}

		if !result.HasDatabase(dbName) {
			t.Fatalf("Node %d does not have database %s", resp.nodeID, dbName)
		}
		t.Logf("Node %d has database %s", resp.nodeID, dbName)
	}

	// When the database is dropped, it should be dropped on all nodes.
	t.Logf("Dropping database %s", dbName)
	if qr := clst.QueryAny(fmat("DROP DATABASE %q", dbName), ""); qr.err != nil {
		t.Fatalf("[node %d] %v", qr.nodeID, qr.err)
	}

	t.Logf("Verifying nodes no longer have database %s", dbName)
	for resp := range clst.QueryAll("SHOW DATABASES", "") {
		result, err := parseResult(ShowDatabases, resp.result)
		if err != nil {
			t.Fatal(err)
		}

		if result.HasDatabase(dbName) {
			t.Errorf("Node %d still has database %s", resp.nodeID, dbName)
		} else {
			t.Logf("Node %d no longer has database %s", resp.nodeID, dbName)
		}
	}
}

// TestShowDropMeasurements tests that a database is available on all data
// nodes, and that when it's dropped, it's dropped from all data nodes.
func TestShowDropMeasurements(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)

	dbName, err := clst.NewDatabase()
	if err != nil {
		t.Fatal(err)
	}

	// Insert a measurement.
	resp := clst.WriteAny(dbName, "cpu,foo=bar value=1", "cpu value=20")
	if resp.err != nil {
		t.Fatal(resp.err)
	}

	// Verify that the cpu measurement is available on all nodes.
	var measurement = "cpu"
	t.Logf("Verifying nodes have measurement %s", measurement)
	for resp := range clst.QueryAll("SHOW MEASUREMENTS", dbName) {
		result, err := parseResult(ShowMeasurements, resp.result)
		if err != nil {
			t.Fatal(err)
		}

		if !result.HasMeasurement(measurement) {
			t.Fatalf("Node %d does not have measurement %s", resp.nodeID, measurement)
		}
		t.Logf("Node %d has measurement %s", resp.nodeID, measurement)
	}

	// When the measurement is dropped, it should be dropped on all
	// nodes.
	t.Logf("Dropping measurement %s", measurement)
	if qr := clst.QueryAny(fmat("DROP MEASUREMENT %q", measurement), dbName); qr.err != nil {
		t.Fatalf("[node %d] %v", qr.nodeID, qr.err)
	}

	t.Logf("Verifying all nodes have dropped measurement %s", measurement)
	for resp := range clst.QueryAll("SHOW MEASUREMENTS", dbName) {
		result, err := parseResult(ShowMeasurements, resp.result)
		if err != nil {
			t.Fatal(err)
		}

		if result.HasMeasurement(measurement) {
			t.Errorf("Node %d still has measurement %s", resp.nodeID, measurement)
		} else {
			t.Logf("Node %d dropped measurement %s", resp.nodeID, measurement)
		}
	}
}

func TestShowDropSeries(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)

	dbName, err := clst.NewDatabase()
	if err != nil {
		t.Fatal(err)
	}

	// Insert some measurements.
	resp := clst.WriteAny(dbName, "cpu,foo=bar value=1", "cpu value=20", "other_measure value=2")
	if resp.err != nil {
		t.Fatal(resp.err)
	}

	// Verify that the cpu series are available on all nodes.
	var seriesMeasure = "cpu"
	t.Logf("Verify nodes have series for measurement %s", seriesMeasure)
	for resp := range clst.QueryAll("SHOW SERIES", dbName) {
		result, err := parseResult(ShowSeries, resp.result)
		if err != nil {
			t.Fatal(err)
		}

		if !result.HasSeriesForMeasurement(seriesMeasure) {
			t.Fatalf("Node %d does not have any series for measurement %s", resp.nodeID, seriesMeasure)
		}
		t.Logf("Node %d has series for measurement %s", resp.nodeID, seriesMeasure)
	}

	t.Logf("Verify nodes no longer have series for measurement %s", seriesMeasure)
	for resp := range clst.QueryAll("SHOW SERIES", dbName) {
		result, err := parseResult(ShowSeries, resp.result)
		if err != nil {
			t.Fatal(err)
		}

		if result.HasSeriesForMeasurement(seriesMeasure) {
			t.Errorf("Node %d still has series for measurement %s", resp.nodeID, seriesMeasure)
		} else {
			t.Logf("Node %d no longer has series for measurement %s", resp.nodeID, seriesMeasure)
		}
	}
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
