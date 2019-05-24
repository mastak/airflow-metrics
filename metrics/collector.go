package metrics

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/process"
)

var cmdR = regexp.MustCompile(`airflow run (?P<dag>\w+) (?P<operator>\w+) (?P<execDate>\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d)`)

// AirflowPythonProcesses returns information about airflow python processes
func AirflowPythonProcesses() ([]*AirflowProcess, error) {
	out := []*AirflowProcess{}
	processes, _ := process.Processes()
	for _, process := range processes {
		cmdline, _ := process.Cmdline()
		if !strings.Contains(cmdline, "airflow run") || !strings.Contains(cmdline, " --raw ") {
			continue
		}
		cmdArgs := findStringSubmatchMap(cmdR, cmdline)
		memInfo, _ := process.MemoryInfoEx()
		memMaps, _ := process.MemoryMaps(true)
		memMap := (*memMaps)[0]

		times, _ := process.Times()
		arflowp := &AirflowProcess{
			dag:       cmdArgs["dag"],
			operator:  cmdArgs["operator"],
			execDate:  cmdArgs["execDate"],
			MemRSS:    float64(memInfo.RSS),
			MemVMS:    float64(memInfo.VMS),
			MemShared: float64(memInfo.Shared),
			MemText:   float64(memInfo.Text),
			MemData:   float64(memInfo.Data),
			MemLib:    float64(memInfo.Lib),
			MemUSS:    float64(memMap.PrivateClean + memMap.PrivateDirty),
			MemPSS:    float64(memMap.Pss),
			MemSwap:   float64(memMap.Swap),
			// CPUPercent:   times.User,
			CPUUser:   times.User,
			CPUSystem: times.System,
		}
		// fmt.Printf("arflowp: %v\n", arflowp)
		out = append(out, arflowp)
	}
	return out, nil
}

// AirflowProcess struct
type AirflowProcess struct {
	dag        string
	operator   string
	execDate   string
	MemRSS     float64
	MemVMS     float64
	MemShared  float64
	MemText    float64
	MemData    float64
	MemLib     float64
	MemUSS     float64
	MemPSS     float64
	MemSwap    float64
	CPUPercent float64
	CPUUser    float64
	CPUSystem  float64
}

// GetLabels returns process lables for prometheus client
func (p AirflowProcess) GetLabels() prometheus.Labels {
	return prometheus.Labels{
		"name":      p.getName(),
		"dag":       p.dag,
		"operator":  p.operator,
		"exec_date": p.execDate,
	}
}

func (p AirflowProcess) getName() string {
	var strs []string

	if strings.Contains(p.operator, p.dag) {
		strs = append(strs, p.operator)
	} else {
		strs = append(strs, fmt.Sprintf("%s.%s", p.operator, p.dag))
	}
	strs = append(strs, p.execDate)
	return strings.Join(strs, "_")
}

func findStringSubmatchMap(r *regexp.Regexp, s string) map[string]string {
	captures := make(map[string]string)

	match := r.FindStringSubmatch(s)
	if match == nil {
		return captures
	}

	for i, name := range r.SubexpNames() {
		// Ignore the whole regexp match and unnamed groups
		if i == 0 || name == "" {
			continue
		}
		captures[name] = match[i]
	}
	return captures
}
