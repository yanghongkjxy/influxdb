package clustertest

import (
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"

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

	t.Skip("Does tests like this one make sense?")
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
			config        = client.BatchPointsConfig{Database: dbName}
			lastNodeWrite int
		)
		resp := clst.WriteAny(config, fmat("%s value=%d", measurement, i))
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

// TestWriteDropShard repeatedly writes to a shard whilst also dropping
// it concurrently.
func TestWriteDropShard(t *testing.T) {
	n := 100

	t.Parallel()
	defer checkPanic(t)

	dbName, err := clst.NewDatabase()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Database %s created", dbName)

	config := client.BatchPointsConfig{Database: dbName}

	resp := clst.WriteAny(config, "cpu value=1")
	if resp.err != nil {
		t.Fatal(resp.err)
	}

	if resp = clst.QueryAny("SHOW SHARDS", ""); resp.err != nil {
		t.Fatal(resp.err)
	}

	result, err := parseResult(ShowShards, resp.result)
	if err != nil {
		t.Fatal(err)
	}

	shards := result.ShardsForDB(dbName)
	if len(shards) != 1 {
		t.Fatalf("%d shards but expected 1", len(shards))
	}
	shardID := shards[0].ID

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			resp := clst.WriteAny(config, "cpu value=1")
			if resp.err != nil {
				t.Fatal(resp.err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n*10; i++ {
			resp := clst.QueryAny(fmat("DROP SHARD %d", shardID), "")
			if resp.err != nil {
				t.Fatal(resp.err)
			}
		}
	}()
	wg.Wait()
}
