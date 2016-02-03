package main

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/client/v2"
)

type Profile struct {
	Queries []*Query `toml:"query"`
	client.Client
}

func (p *Profile) Run() {
	for _, q := range p.Queries {
		q.Exec(p.Client)
		q.Report()
	}
}

func NewProfile(file string) (*Profile, error) {
	p := &Profile{}

	if _, err := toml.DecodeFile(file, p); err != nil {
		return nil, err
	}

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})

	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}

	p.Client = c

	return p, nil
}

type Query struct {
	Statement  string `toml:"statement"`
	Runs       int    `toml:"runs"`
	Database   string `toml:"database"`
	PointCount int    `toml:"point_count"`
	responses  []time.Duration
}

func (q *Query) Exec(c client.Client) {
	for i := 0; i < q.Runs; i++ {
		qry := client.NewQuery(q.Statement, q.Database, "ns")
		t := time.Now()
		if response, err := c.Query(qry); err == nil && response.Error() == nil {
			//fmt.Println(response.Results)
		}
		responseTime := time.Now().Sub(t)

		q.responses = append(q.responses, responseTime)
	}
}

func (q *Query) Mean() time.Duration {
	total := time.Duration(0)
	for _, r := range q.responses {
		total += r
	}

	return total / time.Duration(len(q.responses))
}

func (q *Query) Median() time.Duration {
	responses := make([]int, len(q.responses))
	for i, r := range q.responses {
		responses[i] = int(r)
	}
	sort.IntSlice(responses).Sort()

	return time.Duration(responses[len(q.responses)/2])
}

func (q *Query) StdDev() time.Duration {
	total := time.Duration(0)
	mean := q.Mean()
	for _, r := range q.responses {
		total += (r - mean) * (r - mean)
	}

	return time.Duration(math.Sqrt(float64(total / time.Duration(len(q.responses)))))
}

func (q *Query) Report() {
	fmt.Println(q.Statement)
	fmt.Printf("Average Reponse Time: %v\n", q.Mean())
	fmt.Printf("Standard Deviation: %v\n", q.StdDev())
	fmt.Printf("Median Reponse Time: %v\n", q.Median())

	if q.PointCount != 0 {
		fmt.Printf("Point Per Second: %v\n", int(float64(q.PointCount)/q.Mean().Seconds()))
	}
	fmt.Println()
}

func main() {
	p, err := NewProfile("example/template.toml")
	defer p.Close()
	if err != nil {
		fmt.Println(err)
	}

	p.Run()

	//
	//		t := time.Now()
	//		if response, err := c.Query(q); err == nil && response.Error() == nil {
	//			fmt.Println(response.Results)
	//		}
	//		responseTime := time.Now().Sub(t)
	//
	//		m[qt[i%2]] = append(m[qt[i%2]], responseTime)
	//	}
	//
	//	fmt.Println(m)

}
