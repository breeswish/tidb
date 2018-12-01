package lab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/planner/core"
	"golang.org/x/net/context"
	"io/ioutil"
	"os"
	"strings"
	"time"
	"net/http"
)


const (
	LabEvent_Key = "labEvent_Key"
	Event_Svr_Start = 0
	Event_SQL = 1
	Event_Svr_Stop = 2
)

var event chan Event

var tidbid string

type SQLEvent struct {
	TiDBId  		string 		`json:"tidb_id"`
	Sql  			string 		`json:"sql"`
	PhysicalPlan	interface{} `json:"plan"`
	TxnId			string  	`json:"txn_id"`
}


type EventSvr struct {
	TiDBId  string 		`json:"tidb_id"`
}

type Event struct {
	TS            int64  `json:"timestamp"`
	EvId		  int	 `json:"eid"`
	EventName     string `json:"event_name""`
	Payload       interface{}  `json:"payload""`
}

func (e Event) toString() string {
	bytes, err := json.Marshal(e)
	if err == nil {
		return string(bytes)
	} else {
		panic(err)
	}
}

func pushEvent(e Event) {
	f, err := os.OpenFile("/tmp/testout.log", os.O_APPEND|os.O_WRONLY, 0600)
	if !validEvent(e) {
		return
	}
	defer f.Close()
	if err != nil {
		panic(err)
	}
	data, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	if _, err = f.WriteString(string(data)); err != nil {
		panic(err)
	}
	resp, err := http.Post("http://192.168.198.207:12510/event", "application/json", bytes.NewBuffer(data))
	defer resp.Body.Close()
	if err != nil {
		fmt.Println(err)
	} else {
		data, _ = ioutil.ReadAll(resp.Body)
		f.WriteString(string(data) + "\n")
	}
}

func validEvent(e Event) bool {
	switch e.EvId {
	case Event_SQL:
		sqlEvent, ok := e.Payload.(*SQLEvent)
		if ok {
			if strings.Contains(sqlEvent.Sql, "tikv_gc_last_run_time") {
				return false
			} else {
				return true
			}
		} else {
			return false
		}
	}
	return true
}

func AddEvent(ctx context.Context, eid int) {
	ts := time.Now().UnixNano() / 1000000
	switch eid {
	case Event_Svr_Start:
		event <- Event {
			TS:        ts,
			EvId:      eid,
			EventName: "TiDBStarted",
			Payload:   EventSvr{TiDBId: tidbid},
		}
	case Event_Svr_Stop:
		event <- Event {
			TS:        ts,
			EvId:      eid,
			EventName: "TiDBStopped",
			Payload:   EventSvr{TiDBId: tidbid},
		}
	case Event_SQL:
		sqlEvent, ok := ctx.Value(LabEvent_Key).(*SQLEvent)
		if ok {
			physicalPlan, ok := sqlEvent.PhysicalPlan.(core.PhysicalPlan)
			if ok {
				sqlEvent.PhysicalPlan = physicalPlantoDot(physicalPlan)
			}
			event <- Event {
				TS:            time.Now().UnixNano(),
				EvId:      eid,
				EventName: "TiDBReceivedSQL",
				Payload:   sqlEvent,
			}
		}
	default:
		panic(fmt.Sprintf("Unknown event type %v", eid))
	}
}

func physicalPlantoDot(p core.PhysicalPlan) string {
	buffer := bytes.NewBufferString("")
	buffer.WriteString(fmt.Sprintf("\ndigraph %s {\n", p.ExplainID()))

	toDotHelper(p, "root", buffer)
	buffer.WriteString(fmt.Sprintln("}"))
	return buffer.String()
}

func toDotHelper(p core.PhysicalPlan, taskTp string, buffer *bytes.Buffer) {
	buffer.WriteString(fmt.Sprintf("subgraph cluster%v{\n", p.ID()))
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	buffer.WriteString(fmt.Sprintf("label = \"%s\"\n", taskTp))
	if len(p.Children()) == 0 {
		buffer.WriteString(fmt.Sprintf("\"%s\"\n}\n", p.ExplainID()))
		return
	}

	var copTasks []core.PhysicalPlan
	var pipelines []string

	for planQueue := []core.PhysicalPlan{p}; len(planQueue) > 0; planQueue = planQueue[1:] {
		curPlan := planQueue[0]
		switch copPlan := curPlan.(type) {
		case *core.PhysicalTableReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.TablePlans[0].ExplainID()))
			copTasks = append(copTasks, copPlan.TablePlans[0])
		case *core.PhysicalIndexReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.IndexPlans[0].ExplainID()))
			copTasks = append(copTasks, copPlan.IndexPlans[0])
		case *core.PhysicalIndexLookUpReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.TablePlans[0]))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.IndexPlans[0]))
			copTasks = append(copTasks, copPlan.TablePlans[0])
			copTasks = append(copTasks, copPlan.IndexPlans[0])
		}
		for _, child := range curPlan.Children() {
			buffer.WriteString(fmt.Sprintf("\"%s\" -> \"%s\"\n", curPlan.ExplainID(), child.ExplainID()))
			planQueue = append(planQueue, child)
		}
	}
	buffer.WriteString("}\n")

	for _, cop := range copTasks {
		toDotHelper(cop.(core.PhysicalPlan), "cop", buffer)
	}

	for i := range pipelines {
		buffer.WriteString(pipelines[i])
	}
}


func runLabEventPusher() {
	for {
		var e Event
		e = <- event
		pushEvent(e)
		time.Sleep(1000)
	}
}

func InitLab(host string, port uint) {
	tidbid = fmt.Sprintf("%v:%v", host, port)
	event = make(chan Event)
	go runLabEventPusher()
}



