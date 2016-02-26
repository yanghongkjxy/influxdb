package clustertest

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/client/v2"
)

type serversResult struct {
	MetaServers map[int]string
	DataServers map[int]string
}

// servers parses the client result and gets all data and meta servers.
// It only keeps track of the HTTP bind addresses and node ID.
// TODO(edd) ensure this doesn't panic if wrong result is passed in.
func servers(result client.Result) (*serversResult, error) {
	res := &serversResult{
		MetaServers: make(map[int]string),
		DataServers: make(map[int]string),
	}

	for _, row := range result.Series {
		idIdx, addrIdx := -1, -1
		for i, value := range row.Columns {
			if value == "id" {
				idIdx = i
			} else if value == "http_addr" {
				addrIdx = i
			}
		}

		if idIdx < 0 || addrIdx < 0 {
			return nil, fmt.Errorf("could not get indexes from columns %v", row.Columns)
		}

		var (
			id   int
			addr string
		)

		for _, value := range row.Values {
			id = mustInt(value[idIdx])
			addr = value[addrIdx].(string)
			switch row.Name {
			case "data_nodes":
				res.DataServers[id] = addr
			case "meta_nodes":
				res.MetaServers[id] = addr
			default:
				return nil, fmt.Errorf("unknown row name %q", row.Name)
			}
		}
	}
	return res, nil
}

type databasesResult struct {
	databases []string
}

func (r databasesResult) HasDatabase(name string) bool {
	for _, db := range r.databases {
		if db == name {
			return true
		}
	}
	return false
}

// databases parses the client result and gets all database names.
func databases(result client.Result) (*databasesResult, error) {
	res := &databasesResult{}
	for _, row := range result.Series {
		for _, value := range row.Values {
			if len(value) == 0 {
				return nil, fmt.Errorf("value %v is empty", value)
			}

			name, ok := value[0].(string)
			if !ok {
				return nil, fmt.Errorf("could not parse %v as string", value[0])
			}
			res.databases = append(res.databases, name)
		}
	}
	return res, nil
}

func mustInt(v interface{}) int {
	jnum := v.(json.Number)
	num, err := jnum.Int64()
	if err != nil {
		panic(err)
	}
	return int(num)
}
