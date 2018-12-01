package lab

import (
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"os"
	"strings"
	"time"
)


const (
	LabEvent_Key = "labEvent_Key"
	LabEvent_UserQuery = "LabEvent_UserQuery"
	Event_Svr_Start = 0
	Event_SQL = 1
	Event_Svr_Stop = 2
	Event_Coproc = 3
	Event_Get = 4
	Fuck_Prefix = "_fuck_prefix"
)

var event chan Event

var tidbid string
var host string
var port string

type SQLEvent struct {
	TiDBId  		string 		`json:"tidb_id"`
	Sql  			string 		`json:"sql"`
	PhysicalPlan	string `json:"plan"`
	TxnId			string  	`json:"txn_id"`
}


type EventSvr struct {
	TiDBId  string 		`json:"tidb_id"`
	Host    string		`json:"host"`
	Port    string		`json:"port"`
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
	/*
	resp, err := http.Post("http://192.168.198.207:12510/event", "application/json", bytes.NewBuffer(data))
	defer resp.Body.Close()
	if err != nil {
		fmt.Println(err)
	} else {
		data, _ = ioutil.ReadAll(resp.Body)
		f.WriteString(string(data) + "\n")
	}
	*/
	f.WriteString(string(data) + "\n")
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

func TestUserQuery(ctx context.Context, msg string) bool {
	ret := false
	userQuery, ok := ctx.Value(LabEvent_UserQuery).(bool)
	if ok && userQuery {
		fmt.Println("!!!!!! USERQUERYYY  " + msg)
		ret = true
	} else {
		fmt.Println("!!!!!! NOT " + msg)
	}
	return ret
}

type ReqData struct {
	RegionId uint64 `json:"region_id"`
}

func AddEvent(eid int, data interface{}) {
	ts := time.Now().UnixNano() / 1000000
	switch eid {
	case Event_Svr_Start:
		event <- Event {
			TS:        ts,
			EvId:      eid,
			EventName: "TiDBStarted",
			Payload:   EventSvr {
				TiDBId: tidbid,
				Host: host,
				Port: port,
			},
		}
	case Event_Svr_Stop:
		event <- Event {
			TS:        ts,
			EvId:      eid,
			EventName: "TiDBStopped",
			Payload:   EventSvr{
				TiDBId: tidbid,
				Host: host,
				Port: port,
			},
		}
	case Event_SQL:
		sqlEvent, ok := data.(*SQLEvent)
		if ok {
			event <- Event {
				TS:            time.Now().UnixNano(),
				EvId:      eid,
				EventName: "TiDBReceivedSQL",
				Payload:   sqlEvent,
			}
		}
	case Event_Coproc:
		reqData, ok := data.(*ReqData)
		if ok {
			event <- Event {
				TS:            time.Now().UnixNano(),
				EvId:      eid,
				EventName: "TiDBCoproc",
				Payload:   reqData,
			}
		}
	case Event_Get:
		reqData, ok := data.(*ReqData)
		if ok {
			event <- Event {
				TS:        time.Now().UnixNano(),
				EvId:      eid,
				EventName: "TiDBGet",
				Payload:   reqData,
			}
		}
	default:
		panic(fmt.Sprintf("Unknown event type %v", eid))
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

func InitLab(_host string, _port uint) {
	host = _host
	port = fmt.Sprintf("%v", _port)
	tidbid = fmt.Sprintf("%v:%v", host, port)

	event = make(chan Event)
	go runLabEventPusher()
}



