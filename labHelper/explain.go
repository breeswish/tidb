package labHelper

import (
	"bytes"
	"fmt"
	"github.com/pingcap/tidb/planner/core"
)

// func PhysicalPlantoDot(p core.PhysicalPlan) string {
func PhysicalPlantoDot(_p interface{}) string {
	p, ok := _p.(core.PhysicalPlan)
	if !ok {
		return ""
	}
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
