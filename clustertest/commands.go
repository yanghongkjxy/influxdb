// +build cluster
package clustertest

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/influxdata/influxdb/client/v2"
)

type command int

// Supported commands.
const (
	ShowServers command = iota
	ShowDatabases
	ShowMeasurements
	ShowSeries
	ShowTagKeys
	ShowTagValues
	ShowFieldKeys
)

// commandResult contains parsed data from a query result.
type commandResult struct {
	dataServers  map[int]string
	metaServers  map[int]string
	databases    []string
	measurements []string
	series       map[string][]string
	tagKeys      map[string][]string
	tagValues    map[string][]string
	fieldKeys    map[string][]string
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

// HasSeries returns true if the parsed result contains the provided set
// of series belonging to the provided measurement.
func (r commandResult) HasSeries(measurement string, series []string) bool {
	got, ok := r.series[measurement]
	if !ok {
		// It's OK if series doesn't exist if we're checking for that
		// case.
		return len(series) == 0
	}

	if len(got) != len(series) {
		return false
	}

	sort.Strings(got)
	sort.Strings(series)
	return reflect.DeepEqual(got, series)
}

// HasTagKeys returns true if the result contains a measurement with
// the provided tag key set only.
func (r commandResult) HasTagKeys(measurement string, tagKeys []string) bool {
	got, ok := r.tagKeys[measurement]
	if !ok {
		return false
	}

	if len(got) != len(tagKeys) {
		return false
	}

	sort.Strings(got)
	sort.Strings(tagKeys)
	return reflect.DeepEqual(got, tagKeys)
}

// HasTagValues returns true if the result contains a measurement with
// the provided tag value set only.
func (r commandResult) HasTagValues(measurement string, tagValues []string) bool {
	got, ok := r.tagValues[measurement]
	if !ok {
		return false
	}

	if len(got) != len(tagValues) {
		return false
	}

	sort.Strings(got)
	sort.Strings(tagValues)
	return reflect.DeepEqual(got, tagValues)
}

// HasFieldKeys returns true if the result contains a measurement with
// the provided field key set only.
func (r commandResult) HasFieldKeys(measurement string, fieldKeys []string) bool {
	got, ok := r.fieldKeys[measurement]
	if !ok {
		return false
	}

	if len(got) != len(fieldKeys) {
		return false
	}

	sort.Strings(got)
	sort.Strings(fieldKeys)
	return reflect.DeepEqual(got, fieldKeys)
}

// parseResult parses the client result.
//
// Currently parseResult only supports the results of:
//
// - SHOW SERVERS
// - SHOW DATABASES
// - SHOW MEASUREMENTS
// - SHOW SERIES
// - SHOW TAG KEYS
// - SHOW TAG VALUES
// - SHOW FIELD KEYS
//
func parseResult(c command, result client.Result) (*commandResult, error) {
	res := &commandResult{
		series:    make(map[string][]string),
		tagKeys:   make(map[string][]string),
		tagValues: make(map[string][]string),
		fieldKeys: make(map[string][]string),
	}

	for _, row := range result.Series {
		for _, value := range row.Values {
			if len(value) == 0 {
				return nil, fmt.Errorf("value %v is empty", value)
			}

			// TODO(edd): This is inefficient because we're doing more
			// parsing than we need to in each iteration. But it's
			// conceptually simple for now.
			//
			// At some point this switch will need breaking out into
			// smaller functions.
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
				res.series[row.Name] = append(res.series[row.Name], key)
			case ShowTagKeys:
				tkIDX, err := columnIDX("tagKey", row.Columns)
				if err != nil {
					return nil, err
				}

				tagKey, ok := value[tkIDX].(string)
				if !ok {
					return nil, fmt.Errorf("could not parse %v as string", value[tkIDX])
				}
				res.tagKeys[row.Name] = append(res.tagKeys[row.Name], tagKey)
			case ShowTagValues:
				tvIDX, err := columnIDX("value", row.Columns)
				if err != nil {
					return nil, err
				}

				tagValue, ok := value[tvIDX].(string)
				if !ok {
					return nil, fmt.Errorf("could not parse %v as string", value[tvIDX])
				}
				res.tagValues[row.Name] = append(res.tagValues[row.Name], tagValue)
			case ShowFieldKeys:
				fkIDX, err := columnIDX("fieldKey", row.Columns)
				if err != nil {
					return nil, err
				}

				fieldKey, ok := value[fkIDX].(string)
				if !ok {
					return nil, fmt.Errorf("could not parse %v as string", value[fkIDX])
				}
				res.fieldKeys[row.Name] = append(res.fieldKeys[row.Name], fieldKey)
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
	return -1, fmt.Errorf("can't find column called %q", s)
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
