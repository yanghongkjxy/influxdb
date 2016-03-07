package clustertest

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"testing"
	"time"
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
	inspect := flag.Bool("inspect", false, "inspect will pause the main test goroutine on a test fail")
	flag.Parse()

	var err error
	// Let's create a cluster of 3 hybrid nodes. Here we could switch
	// this out for a remote cluster.
	if clst, err = newlocal(*hybridN, *metaN, *dataN, *binPath); err != nil {
		panic(err)
	}

	if err = clst.Start(); err != nil {
		clst.Stop()
		panic(err)
	}

	code := m.Run()

	if code > 0 && *inspect {
		c := make(chan os.Signal, 1)
		fmt.Println("Pausing test process for inspection. Send SIGTERM to exit (Ctrl + c)")
		signal.Notify(c, os.Interrupt)
		<-c
		fmt.Println("SIGTERM received. Shutting down gracefully.")
	}
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
	t.Logf("Using database: %s", dbName)

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

// TestDropMeasurement tests that a measurement written to all data nodes
// is dropped from all data nodes.
func TestDropMeasurement(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)

	dbName, err := clst.NewDatabase()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Using database: %s", dbName)

	// Insert a measurement.
	var measurement = "cpu"
	resp := clst.WriteAny(dbName, fmat("%s value=1", measurement))
	if resp.err != nil {
		t.Fatal(resp.err)
	}
	t.Logf("[node %d] point written.", resp.nodeID)

	// Verify that the measurement is available on all nodes.
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

// TestShowMeasurements tests that a measurement is available on all
// data nodes when it's only been written to a single node.
func TestShowMeasurements(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)

	dbName, err := clst.NewDatabase(withDefaultRP(time.Hour, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Using database: %s", dbName)

	// Insert a measurement. It will only be stored on a single node
	// due to the retention policy.
	var measurement = "cpu"
	resp := clst.WriteAny(dbName, fmat("%s value=1", measurement))
	if resp.err != nil {
		t.Fatal(resp.err)
	}
	t.Logf("[node %d] point written.", resp.nodeID)

	// Verify that the cpu measurement is available on all nodes.
	t.Logf("Verifying all nodes have measurement %s", measurement)
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
}

// TestDropSeries tests that a series written to all nodes is dropped
// from all nodes.
func TestDropSeries(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)

	dbName, err := clst.NewDatabase()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Using database: %s", dbName)

	// Insert some measurements.
	var seriesMeasure = "cpu"
	resp := clst.WriteAny(dbName,
		fmat("%s,foo=bar value=1", seriesMeasure),
		fmat("%s value=20", seriesMeasure),
		"other_measure value=2",
	)
	if resp.err != nil {
		t.Fatal(resp.err)
	}
	t.Logf("[node %d] point written.", resp.nodeID)

	// Verify that the cpu series are available on all nodes.
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

	// Drop the series..
	t.Logf("Dropping all series for measurement %s", seriesMeasure)
	if qr := clst.QueryAny(fmat("DROP SERIES FROM %q", seriesMeasure), dbName); qr.err != nil {
		t.Fatalf("[node %d] %v", qr.nodeID, qr.err)
	}

	// Verify the series have been removed from
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

// TestShowSeries tests that a series is available on all data nodes
// when it's only been written to a single node.
func TestShowSeries(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)
	t.Skip("Waiting on some work")

	// Create a database with a retentention policy that ensure data
	// only written to one node.
	dbName, err := clst.NewDatabase(withDefaultRP(time.Hour, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Using database: %s", dbName)

	// Insert a series.
	var seriesMeasure = "cpu"
	resp := clst.WriteAny(dbName, fmat("%s value=1", seriesMeasure))
	if resp.err != nil {
		t.Fatal(resp.err)
	}
	t.Logf("[node %d] point written.", resp.nodeID)

	// Verify that the cpu series is available on all nodes.
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
}

// TestShowTagKeys tests that tags keys for a series are available from
// any data node in a cluster.
func TestShowTagKeys(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)

	// Create a database with a retention policy that ensure data
	// only written to one node.
	dbName, err := clst.NewDatabase(withDefaultRP(time.Hour, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Using database: %s", dbName)

	// Write some series with tag keys.
	var seriesMeasures = []string{"cpu", "memory"}
	resp := clst.WriteAny(dbName,
		fmat("%s,foo=bar,zah=zoo value=1", seriesMeasures[0]),
		fmat("%s,a=b value=20", seriesMeasures[1]),
	)
	if resp.err != nil {
		t.Fatal(resp.err)
	}
	t.Logf("[node %d] point written.", resp.nodeID)

	// Verify that the tag keys are available on all nodes.
	expectedKeys := map[string][]string{
		seriesMeasures[0]: []string{"foo", "zah"},
		seriesMeasures[1]: []string{"a"},
	}

	t.Log("Verify nodes have tag keys for all written series")
	for resp := range clst.QueryAll("SHOW TAG KEYS", dbName) {
		result, err := parseResult(ShowTagKeys, resp.result)
		if err != nil {
			t.Fatal(err)
		}

		for measure, tks := range expectedKeys {
			if !result.HasTagKeys(measure, tks) {
				t.Fatalf("Node %d does not have tag keys %v for measurement %s", resp.nodeID, tks, measure)
			}
			t.Logf("Node %d has all expected tag keys for measurement %s", resp.nodeID, measure)
		}

	}
}

func TestShowTagValues(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)
	t.Skip("Waiting on some work")

	// Create a database with a retention policy that ensure data
	// only written to one node.
	dbName, err := clst.NewDatabase(withDefaultRP(time.Hour, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Using database: %s", dbName)

	// Write some series with tag values.
	var seriesMeasures = []string{"cpu", "memory"}
	resp := clst.WriteAny(dbName,
		fmat("%s,foo=bar,zah=zoo value=1", seriesMeasures[0]),
		fmat("%s,a=b value=20", seriesMeasures[1]),
	)
	if resp.err != nil {
		t.Fatal(resp.err)
	}
	t.Logf("[node %d] point written.", resp.nodeID)

	// Verify that the tag keys are available on all nodes.
	expectedValues := map[string][]string{
		seriesMeasures[0]: []string{"bah", "zoo"},
		seriesMeasures[1]: []string{"b"},
	}

	t.Log("Verify nodes have tag values for all written series")
	for resp := range clst.QueryAll("SHOW TAG VALUES", dbName) {
		result, err := parseResult(ShowTagValues, resp.result)
		if err != nil {
			t.Fatal(err)
		}

		for measure, tvs := range expectedValues {
			if !result.HasTagKeys(measure, tvs) {
				t.Fatalf("Node %d does not have tag values %v for measurement %s", resp.nodeID, tvs, measure)
			}
			t.Logf("Node %d has all expected tag values for measurement %s", resp.nodeID, measure)
		}

	}
}

// TestShowFieldKeys tests that tags keys for a series are available from
// any data node in a cluster.
func TestShowFieldKeys(t *testing.T) {
	t.Parallel()
	defer checkPanic(t)

	// Create a database with a retention policy that ensure data
	// only written to one node.
	dbName, err := clst.NewDatabase(withDefaultRP(time.Hour, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Using database: %s", dbName)

	// Write some series with tag keys.
	var seriesMeasures = []string{"cpu", "memory"}
	resp := clst.WriteAny(dbName,
		fmat(`%s value=1,boo="zoo"`, seriesMeasures[0]),
		fmat("%s,a=b power=20", seriesMeasures[1]),
	)
	if resp.err != nil {
		t.Fatal(resp.err)
	}
	t.Logf("[node %d] point written.", resp.nodeID)

	// Verify that the tag keys are available on all nodes.
	expectedKeys := map[string][]string{
		seriesMeasures[0]: []string{"value", "boo"},
		seriesMeasures[1]: []string{"power"},
	}

	t.Log("Verify nodes have tag keys for all written series")
	for resp := range clst.QueryAll("SHOW FIELD KEYS", dbName) {
		result, err := parseResult(ShowFieldKeys, resp.result)
		if err != nil {
			t.Fatal(err)
		}

		for measure, fks := range expectedKeys {
			if !result.HasFieldKeys(measure, fks) {
				t.Fatalf("Node %d does not have field keys %v for measurement %s", resp.nodeID, fks, measure)
			}
			t.Logf("Node %d has all expected tag keys for measurement %s", resp.nodeID, measure)
		}

	}
}

func fmat(f string, v ...interface{}) string {
	return fmt.Sprintf(f, v...)
}
