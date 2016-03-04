package clustertest

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/uuid"
)

// TestCreateDropDatabase repeatedly creates a database in a
// cluster and then drops it.
func TestCreateDropDatabase(t *testing.T) {
	n := 100

	t.Parallel()
	defer checkPanic(t)

	dbName := uuid.TimeUUID().String()
	t.Logf("Database %s created", dbName)
	for i := 0; i < n; i++ {
		// Create the database using any node.
		if resp := clst.QueryAny(fmat("CREATE DATABASE %q", dbName), "§"); resp.err != nil {
			t.Fatal(resp.err)
		}

		// Drop the database using any node.
		if resp := clst.QueryAny(fmat("DROP DATABASE %q", dbName), ""); resp.err != nil {
			t.Fatal(resp.err)
		}

	}
}

// TestCreateWriteShowMeasurements repeatedly creates a database in a
// cluster with a replication factor of 1, then attempts runs
// SHOW MEASUREMENTS on all nodes in the cluster.
func TestCreateWriteShowMeasurements(t *testing.T) {
	n := 100

	t.Parallel()
	defer checkPanic(t)

	for i := 0; i < n; i++ {
		dbName, err := clst.NewDatabase(withDefaultRP(time.Hour, 1))
		if err != nil {
			t.Fatal(err)
		}

		// Insert a measurement. It will only be stored on a single node
		// due to the retention policy.
		var (
			measurement   = "cpu"
			lastNodeWrite int
		)
		resp := clst.WriteAny(dbName, fmat("%s value=1", measurement))
		if resp.err != nil {
			t.Fatal(resp.err)
		}
		lastNodeWrite = resp.nodeID

		// Verify that the cpu measurement is available on all nodes.
		for resp := range clst.QueryAll("SHOW MEASUREMENTS", dbName) {
			result, err := parseResult(ShowMeasurements, resp.result)
			if err != nil {
				t.Fatal(err)
			}

			if !result.HasMeasurement(measurement) {
				t.Fatalf("Node %d does not have measurement %s on database %s (data written to %d)", resp.nodeID, measurement, dbName, lastNodeWrite)
			}
		}
	}
}
