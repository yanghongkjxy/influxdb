package clustertest

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/services/httpd"

	"github.com/influxdata/influxdb/services/meta"

	"github.com/influxdata/influxdb/cmd/influxd/run"
)

// Gap between ports on different nodes, e.g., 8188, 8288, 8388
const portJump = 100

// Cluster is the interface to a running Influxd cluster.
type Cluster interface {
	// Start initialises the cluster.
	Start() error

	// Close releases any necessary resources.
	Close() error

	// RunAll runs the provided query on all data nodes in the cluster.
	RunAll(cmd string, database string) <-chan queryResponse

	// RunAny runs the provided query on an arbitrarily chosen node in the
	// cluster.
	RunAny(cmd string, database string) queryResponse

	// Run the query on the specified data node.
	Run(id int, cmd string, database string) queryResponse

	// NewDatabase generates a database name and creates it in the
	// cluster.
	NewDatabase() (string, error)
}

// TODO(edd): implement a remote Cluster?
type remote struct{}

// local is a locally running Cluster.
type local struct {
	// Cluster base directory. All nodes will live in n[i] folders
	// under this path.
	baseDir string

	// Location of influxd binary.
	binPath string

	// Number of expected meta and data nodes in the cluster.
	metaN, dataN int

	// All configurations created for the cluster
	nodeConfs map[string]*run.Config

	// Entry point into the cluster. This node is queried to check all
	// other nodes are up and ready.
	entryAddr string

	// queryTimeout specifies how long to wait before timing out a query
	queryTimeout time.Duration

	mu      sync.RWMutex
	clients map[int]client.Client // clients for querying nodes
	dbi     int                   // Index for ensuring we can create a new database

	// dataNodes maintains a mapping between a data/hybrid node's ID and
	// the address their HTTP service is bound to.
	dataNodes map[int]string

	// cmds keeps track of all the processes we started.
	cmds []*exec.Cmd

	logger *log.Logger
}

// newlocal creates configuration files and directories for a new
// local cluster.
//
// The location to the influxd binary must be provided. If the empty
// string is passed in then `influxd` will be used. When the caller is
// ready to start all nodes in the cluster call Start.
//
// NB (edd) temporary cluster directory will not be cleaned up at the
// moment, but currently it's using host OS's temp directory.
func newlocal(hybridN, metaN, dataN int, binPath string) (*local, error) {
	if metaN+hybridN < 3 {
		panic("cluster must have at least three meta nodes")
	} else if dataN+hybridN < 1 {
		panic("cluster must have at least one data node")
	}

	c := &local{
		binPath:      binPath,
		metaN:        hybridN + metaN,
		dataN:        hybridN + dataN,
		nodeConfs:    make(map[string]*run.Config),
		entryAddr:    "localhost" + mustShiftPort(httpd.DefaultBindAddress, portJump),
		queryTimeout: 10 * time.Second,
		clients:      make(map[int]client.Client),
		logger:       log.New(os.Stdout, "", log.LstdFlags),
	}

	// Location of cluster files.
	var err error
	if c.baseDir, err = ioutil.TempDir("", "influx-integration"); err != nil {
		panic(err)
	}
	log.Println(c.baseDir)

	// Setup configuration files.
	joinArg := generateJoinArg(metaN+hybridN, mustptoi(meta.DefaultHTTPBindAddress))

	// TODO(edd): DRY these loops up.
	for i := 1; i <= hybridN; i++ {
		nodePath := path.Join(c.baseDir, fmt.Sprintf("n%d", i))
		conf := newConfig(nodePath, i, "hybrid")
		conf.Join = joinArg
		c.nodeConfs[path.Join(nodePath, "config.toml")] = conf
	}

	for i := 1; i <= dataN; i++ {
		id := i + hybridN
		nodePath := path.Join(c.baseDir, fmt.Sprintf("n%d", id))
		conf := newConfig(nodePath, id, "data")
		conf.Join = joinArg
		c.nodeConfs[path.Join(nodePath, "config.toml")] = conf
	}

	for i := 1; i <= metaN; i++ {
		id := i + hybridN + dataN
		nodePath := path.Join(c.baseDir, fmt.Sprintf("n%d", id))
		conf := newConfig(nodePath, id, "meta")
		conf.Join = joinArg
		c.nodeConfs[path.Join(nodePath, "config.toml")] = conf
	}

	// Write out config files
	for pth, conf := range c.nodeConfs {
		// Generate the directories.
		if err := os.MkdirAll(path.Dir(pth), os.ModePerm); err != nil {
			return nil, err
		}

		fd, err := os.Create(pth)
		if err != nil {
			return nil, err
		}

		err = toml.NewEncoder(fd).Encode(conf)
		fd.Close()
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Start starts all of the Influxd processes in the cluster.
func (c *local) Start() error {
	if err := c.startNodes(); err != nil {
		return err
	}
	if err := c.mapServers(20 * time.Second); err != nil {
		return err
	}
	return nil
}

// TODO(edd) Provide ability to do something with stdout and stderr
// of the process.
func (c *local) startNodes() error {
	if c.binPath == "" {
		c.binPath = "influxd"
	}
	for pth := range c.nodeConfs {
		cmd := exec.Command(c.binPath, fmt.Sprintf("-config=%s", pth))
		// cmd.Stdout = os.Stdout
		// cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return err
		}
		c.cmds = append(c.cmds, cmd)
	}
	return nil
}

// mapServers generates a mapping between node ids and the ports they're
// HTTP services are listening on.
func (c *local) mapServers(timeout time.Duration) error {
	// We need to use the entry point to the cluster, as we don't know
	// about any other nodes yet.
	clt, err := client.NewHTTPClient(client.HTTPConfig{Addr: "http://" + c.entryAddr})
	if err != nil {
		return err
	}
	defer clt.Close()

	var (
		check   <-chan time.Time
		retries float64
		tc      = time.After(timeout)
	)

	check = time.After(0)
	for {
		select {
		case <-tc:
			return fmt.Errorf("timed out waiting for all servers")
		case <-check:
			resp, err := clt.Query(client.NewQuery("SHOW SERVERS", "", ""))
			if err != nil {
				d := time.Duration(math.Pow(2, retries)) * 50 * time.Millisecond
				check = time.After(d)
				c.logger.Printf("Retrying after %v: %v", d, err)
				retries++
				continue
			}

			if resp.Error() != nil {
				return resp.Error()
			}

			if len(resp.Results) == 0 {
				return fmt.Errorf("expected some results")
			}

			result, err := servers(resp.Results[0])
			if err != nil || len(result.MetaServers) < c.metaN || len(result.DataServers) < c.dataN {
				d := time.Duration(math.Pow(2, retries)) * 50 * time.Millisecond
				check = time.After(d)
				c.logger.Printf("Retrying after %v: %v, %d, %d\n", d, err, len(result.MetaServers), len(result.DataServers))
				retries++
				continue
			}
			c.dataNodes = result.DataServers
			return nil
		}
	}
}

// Close terminates all influxd processes started by the suite.
func (c *local) Close() error {
	c.logger.Print("Killing influxd processes")
	for _, cmd := range c.cmds {
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			// TODO(edd): should really go to stderr (or logger for errors)
			c.logger.Print(err)
		}
	}
	return nil
}

type queryResponse struct {
	nodeID int
	result client.Result
	err    error
}

// RunAll runs the query on all data nodes in the cluster. RunAll
// immediately returns a channel to the caller, and ensures that the
// channel is closed when all nodes have returned results.
func (c *local) RunAll(cmd string, database string) <-chan queryResponse {
	var (
		ch = make(chan queryResponse, len(c.dataNodes))
		wg sync.WaitGroup
	)

	for id := range c.dataNodes {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ch <- c.Run(id, cmd, database)
		}(id)
	}

	// Close the channel when all results have been gathered
	go func() { wg.Wait(); close(ch) }()
	return ch
}

func (c *local) RunAny(cmd string, database string) queryResponse {
	var chosen int
	for id := range c.dataNodes {
		chosen = id
		break
	}
	return c.Run(chosen, cmd, database)
}

// Run runs the query on the specified data node.
func (c *local) Run(id int, cmd string, database string) queryResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	resp := queryResponse{nodeID: id}

	// Get client for node.
	clt, ok := c.clients[id]
	if !ok {
		conf := client.HTTPConfig{Timeout: c.queryTimeout, Addr: "http://" + c.dataNodes[id]}
		if clt, resp.err = client.NewHTTPClient(conf); resp.err != nil {
			return resp
		}
		c.clients[id] = clt
	}

	// TODO(edd): handle precision?
	qr, err := clt.Query(client.NewQuery(cmd, database, ""))
	if err != nil {
		resp.err = err
		return resp
	}

	if qr.Error() != nil {
		resp.err = qr.Error()
		return resp
	}

	if len(qr.Results) == 0 {
		resp.err = fmt.Errorf("expected some results")
		return resp
	}

	resp.result = qr.Results[0]
	return resp
}

// NewDatabase generates a new database name of the form `dbx` where x
// is an increasing number, and creates it within the cluster.
func (c *local) NewDatabase() (string, error) {
	c.mu.Lock()
	c.dbi++
	name := fmt.Sprintf("db%d", c.dbi)
	c.mu.Unlock()

	qr := c.RunAny(fmt.Sprintf("CREATE DATABASE %q", name), "")
	if qr.err != nil {
		return "", qr.err
	}
	return name, nil
}

// generateJoinArg generate a join string for joining to all the meta
// nodes in the cluster.
func generateJoinArg(n int, seedPort int) string {
	var join string
	for i := portJump; i <= n*portJump; i += portJump {
		join += fmt.Sprintf("localhost:%d", i+seedPort)
		if i+portJump <= n*portJump {
			join += ","
		}
	}
	return join
}
