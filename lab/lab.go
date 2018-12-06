package lab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/tidb/config"
	"golang.org/x/net/context"
)

const (
	LabEvent_Key       = "labEvent_Key"
	LabEvent_UserQuery = "LabEvent_UserQuery"
	Event_Svr_Start    = 0
	Event_SQL          = 1
	Event_Svr_Stop     = 2
	Event_Coproc       = 3
	Event_Get          = 4
	Event_Commit       = 5

	Fuck_Prefix = "_fuck_prefix"
)

var event chan Event

var tidbid string
var host string
var port string

type SQLEvent struct {
	TiDBId       string `json:"tidb_id"`
	Sql          string `json:"sql"`
	PhysicalPlan string `json:"plan"`
	TxnId        string `json:"txn_id"`
	Kind         string `json:"kind"`
}

type EventSvr struct {
	TiDBId string `json:"tidb_id"`
	Host   string `json:"host"`
	Port   string `json:"port"`
}

type Event struct {
	TS        int64       `json:"timestamp"`
	EvId      int         `json:"eid"`
	EventName string      `json:"event_name""`
	Payload   interface{} `json:"payload""`
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
	cfg := config.GetGlobalConfig()
	if len(cfg.LabAddress) == 0 {
		return
	}
	data, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/event", cfg.LabAddress), "application/json", bytes.NewBuffer(data))
	defer resp.Body.Close()
	if err != nil {
		fmt.Println(err)
	}
}

func chooseKind(sql string) string {
	sql = strings.ToLower(sql)
	if strings.Contains(sql, "insert") ||
		strings.Contains(sql, "update") ||
		strings.Contains(sql, "delete") {
		return "write"
	}
	return "read"
}

func TestUserQuery(ctx context.Context, msg string) (bool, string) {
	ret := false
	userQuery, ok := ctx.Value(LabEvent_UserQuery).(string)
	if ok && len(userQuery) != 0 {
		// fmt.Println("!!!!!! USERQUERYYY  " + msg)
		ret = true
	} else {
		// fmt.Println("!!!!!! NOT " + msg)
	}
	return ret, userQuery
}

type ReqData struct {
	TidbId   string `json:"tidb_id"`
	RegionId uint64 `json:"region_id"`
	StoreId  uint64 `json:"store_id"`
	Sql      string `json:"sql"`
}

func AddEvent(eid int, data interface{}) {
	ts := time.Now().UnixNano() / 1000000
	switch eid {
	case Event_Svr_Start:
		event <- Event{
			TS:        ts,
			EvId:      eid,
			EventName: "TiDBStarted",
			Payload: EventSvr{
				TiDBId: tidbid,
				Host:   host,
				Port:   port,
			},
		}
	case Event_Svr_Stop:
		event <- Event{
			TS:        ts,
			EvId:      eid,
			EventName: "TiDBStopped",
			Payload: EventSvr{
				TiDBId: tidbid,
				Host:   host,
				Port:   port,
			},
		}
	case Event_SQL:
		sqlEvent, ok := data.(*SQLEvent)
		sqlEvent.TiDBId = tidbid
		sqlEvent.Kind = chooseKind(sqlEvent.Sql)
		if ok {
			event <- Event{
				TS:        time.Now().UnixNano(),
				EvId:      eid,
				EventName: "TiDBReceivedSQL",
				Payload:   sqlEvent,
			}
		}
	case Event_Coproc:
		reqData, ok := data.(*ReqData)
		reqData.TidbId = tidbid
		if ok {
			event <- Event{
				TS:        time.Now().UnixNano(),
				EvId:      eid,
				EventName: "TiDBCoproc",
				Payload:   reqData,
			}
		}
	case Event_Get:
		reqData, ok := data.(*ReqData)
		reqData.TidbId = tidbid
		if ok {
			event <- Event{
				TS:        time.Now().UnixNano(),
				EvId:      eid,
				EventName: "TiDBGet",
				Payload:   reqData,
			}
		}
	case Event_Commit:
		reqData, ok := data.(*ReqData)
		reqData.TidbId = tidbid
		if ok {
			event <- Event{
				TS:        time.Now().UnixNano(),
				EvId:      eid,
				EventName: "TiDBCommit",
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
		e = <-event
		pushEvent(e)
	}
}

func InitLab(_host string, _port uint) {
	host = _host
	port = fmt.Sprintf("%v", _port)
	tidbid = fmt.Sprintf("%v:%v", host, port)

	event = make(chan Event)
	go runLabEventPusher()
}
