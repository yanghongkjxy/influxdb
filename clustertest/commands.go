package clustertest

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/client/v2"
)

type command int

// Supported commands.
const (
	ShowServers command = iota
	ShowDatabases
	ShowMeasurements
	ShowSeries
)

// commandResult contains parsed data from a query result.
type commandResult struct {
	dataServers  map[int]string
	metaServers  map[int]string
	databases    []string
	measurements []string
	series       map[string][]string
}

// HasDatabase determines if the parsed result contains a database
// called name.
func (r commandResult) HasDatabase(name string) bool {
	for _, db := range r.databases {
		if db == name {
			return true
		}
	}
	return false
}

// HasMeasurement determines if the parsed result contains a measurement
// called name
func (r commandResult) HasMeasurement(name string) bool {
	for _, m := range r.measurements {
		if m == name {
			return true
		}
	}
	return false
}

// HasSeriesForMeasurement returns true if the parsed result contains
// any series that belongs to the measurement specified by name.
func (r commandResult) HasSeriesForMeasurement(name string) bool {
	_, ok := r.series[name]
	return ok
}

// parseResult parses the client result.
//
// Currently parseResult only supports the results of:
//
// - SHOW DATABASES
// - SHOW MEASUREMENTS
//
func parseResult(c command, result client.Result) (*commandResult, error) {
	res := &commandResult{}
	for _, row := range result.Series {
		for _, value := range row.Values {
			if len(value) == 0 {
				return nil, fmt.Errorf("value %v is empty", value)
			}

			switch c {
			case ShowServers:
				idIDX, err := columnIDX("id", row.Columns)
				if err != nil {
					return nil, err
				}

				addrIDX, err := columnIDX("http_addr", row.Columns)
				if err != nil {
					return nil, err
				}

				id, err := toInt(value[idIDX])
				if err != nil {
					return nil, err
				}

				addr, ok := value[addrIDX].(string)
				if !ok {
					return nil, fmt.Errorf("could not parse %v as string", value[addrIDX])
				}

				switch row.Name {
				case "data_nodes":
					if res.dataServers == nil {
						res.dataServers = make(map[int]string)
					}
					res.dataServers[id] = addr
				case "meta_nodes":
					if res.metaServers == nil {
						res.metaServers = make(map[int]string)
					}
					res.metaServers[id] = addr
				default:
					return nil, fmt.Errorf("unknown row name %q", row.Name)
				}
			case ShowDatabases:
				idx, err := columnIDX("name", row.Columns)
				if err != nil {
					return nil, err
				}

				name, ok := value[idx].(string)
				if !ok {
					return nil, fmt.Errorf("could not parse %v as string", value[idx])
				}

				res.databases = append(res.databases, name)
			case ShowMeasurements:
				idx, err := columnIDX("name", row.Columns)
				if err != nil {
					return nil, err
				}

				name, ok := value[idx].(string)
				if !ok {
					return nil, fmt.Errorf("could not parse %v as string", value[idx])
				}
				res.measurements = append(res.measurements, name)
			case ShowSeries:
				idx, err := columnIDX("_key", row.Columns)
				if err != nil {
					return nil, err
				}

				key, ok := value[idx].(string)
				if !ok {
					return nil, fmt.Errorf("could not parse %v as string", value[idx])
				}

				if res.series == nil {
					res.series = make(map[string][]string)
				}
				res.series[row.Name] = append(res.series[row.Name], key)
			default:
				panic("unable to parse this command")
			}
		}
	}
	return res, nil
}

// columnIDX determines which series index refers to the column named s.
func columnIDX(s string, columns []string) (int, error) {
	for i, col := range columns {
		if col == s {
			return i, nil
		}
	}
	return -1, fmt.Errorf("can't find column called %q")
}

// toInt asserts an int out of a json.Number
func toInt(v interface{}) (int, error) {
	jnum, ok := v.(json.Number)
	if !ok {
		return 0, fmt.Errorf("could not assert %[1]v (%[1]T) as json.Number", v)
	}

	num, err := jnum.Int64()
	if err != nil {
		return 0, err
	}
	return int(num), nil
}
