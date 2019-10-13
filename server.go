package main

import (
	"flag"
	"fmt"
	"github.com/shirou/gopsutil/process"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	addr         = flag.String("web.listen-address", ":8080", "Server address")
	interval     = flag.Int("interval", 10, "Interval fo metrics collection in seconds")
	verbose      = flag.Bool("verbose", false, "Add more logs")
	customLabels = flag.String("labels", "", "Custom labels")
	hostnamePath = flag.String("hostname-path", "", "Path to file with hostname")
)

func main() {
	flag.Parse()
	registry := prometheus.NewRegistry()
	constLabels := getConstLabels()
	fields := createMetricsFields(registry, constLabels)
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	go readMetrics(fields)

	// http.Handle("/metrics", prometheus.Handler()) includes go metrics
	http.Handle("/metrics", handler)
	log.Printf("Starting web server at %s\n", *addr)
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Printf("http.ListenAndServer: %v\n", err)
	}
}

type metricsFields struct {
	memRSS     *prometheus.GaugeVec
	memVMS     *prometheus.GaugeVec
	memShared  *prometheus.GaugeVec
	memText    *prometheus.GaugeVec
	memData    *prometheus.GaugeVec
	memLib     *prometheus.GaugeVec
	memUSS     *prometheus.GaugeVec
	memPSS     *prometheus.GaugeVec
	memSwap    *prometheus.GaugeVec
	cpuPercent *prometheus.GaugeVec
	cpuUser    *prometheus.GaugeVec
	cpuSystem  *prometheus.GaugeVec
}

func createMetricsFields(registry *prometheus.Registry, constLabels prometheus.Labels) *metricsFields {
	defNewGaugeVec := func(name string, help string) *prometheus.GaugeVec {
		vec := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        name,
				Help:        help,
				ConstLabels: constLabels,
			},
			[]string{"name", "dag", "operator", "exec_date"})
		registry.MustRegister(vec)
		return vec
	}

	return &metricsFields{
		memRSS:    defNewGaugeVec("airflow_process_mem_rss", "Non-swapped physical memory"),
		memVMS:    defNewGaugeVec("airflow_process_mem_vms", "Amount of virtual memory"),
		memShared: defNewGaugeVec("airflow_process_mem_shared", "Amount of shared memory"),
		memText:   defNewGaugeVec("airflow_process_mem_text", "Devoted to executable code"),
		memData:   defNewGaugeVec("airflow_process_mem_data", "amount of physical memory devoted to other than executable code"),
		memLib:    defNewGaugeVec("airflow_process_mem_lib", "Used by shared libraries"),
		memUSS:    defNewGaugeVec("airflow_process_mem_uss", "Mem unique to a process and which would be freed"),
		memPSS: defNewGaugeVec("airflow_process_mem_pss", `Shared with other processes, accounted in a way that
	the amount is divided evenly between processes that share it`),
		memSwap:    defNewGaugeVec("airflow_process_mem_swap", "Amount of swapped memory"),
		cpuPercent: defNewGaugeVec("airflow_process_cpu_percent", "System-wide CPU utilization as a percentage of the process"),
		cpuUser:    defNewGaugeVec("airflow_process_cpu_times_user", "CPU times user"),
		cpuSystem:  defNewGaugeVec("airflow_process_cpu_times_system", "CPU times system"),
	}
}

func readMetrics(fields *metricsFields) {
	var labels prometheus.Labels
	for {
		fields.memRSS.Reset()
		fields.memVMS.Reset()
		fields.memShared.Reset()
		fields.memText.Reset()
		fields.memData.Reset()
		fields.memLib.Reset()
		fields.memUSS.Reset()
		fields.memPSS.Reset()
		fields.memSwap.Reset()
		fields.cpuPercent.Reset()
		fields.cpuUser.Reset()
		fields.cpuSystem.Reset()

		airflowProcesses, err := AirflowPythonProcesses()
		for _, process := range airflowProcesses {
			if *verbose {
				fmt.Printf("Process: %v\n", process)
			}
			labels = process.GetLabels()
			fields.memRSS.With(labels).Set(process.MemRSS)
			fields.memVMS.With(labels).Set(process.MemVMS)
			fields.memShared.With(labels).Set(process.MemShared)
			fields.memText.With(labels).Set(process.MemText)
			fields.memData.With(labels).Set(process.MemVMS)
			fields.memLib.With(labels).Set(process.MemLib)
			fields.memUSS.With(labels).Set(process.MemUSS)
			fields.memPSS.With(labels).Set(process.MemPSS)
			fields.memSwap.With(labels).Set(process.MemSwap)
			fields.cpuPercent.With(labels).Set(process.CPUUser)
			fields.cpuUser.With(labels).Set(process.CPUUser)
			fields.cpuSystem.With(labels).Set(process.CPUSystem)
		}

		if err != nil {
			log.Printf("http.ListenAndServer: %v\n", err)
			return
		}
		time.Sleep(time.Duration(*interval) * time.Second)
	}
}

func getConstLabels() prometheus.Labels {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = ""
	}
	constLabels := prometheus.Labels{
		"hostname": hostname,
	}

	if *hostnamePath != "" {
		content, err := ioutil.ReadFile(*hostnamePath)
		if err == nil {
			constLabels["host_hostname"] = string(content)
		}
	}

	if *customLabels != "" {
		for _, customLabel := range strings.Split(*customLabels, ",") {
			constLabels[customLabel] = "true"
		}
	}

	fmt.Println("constLabels:", constLabels)
	return constLabels
}


var cmdR = regexp.MustCompile(`airflow run (?P<dag>\w+) (?P<operator>\w+) (?P<execDate>\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d)`)

// AirflowPythonProcesses returns information about airflow python processes
func AirflowPythonProcesses() ([]*AirflowProcess, error) {
	out := []*AirflowProcess{}
	processes, _ := process.Processes()
	for _, process := range processes {
		cmdline, _ := process.Cmdline()
		fmt.Printf("CMDLINE: %+v\n", cmdline)
		if !strings.Contains(cmdline, "airflow run") || !strings.Contains(cmdline, " --raw ") {
			continue
		}
		cmdArgs := findStringSubmatchMap(cmdR, cmdline)
		memInfo, _ := process.MemoryInfoEx()
		memMaps, _ := process.MemoryMaps(true)
		memMap := (*memMaps)[0]

		times, _ := process.Times()
		airflowp := &AirflowProcess{
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
		// fmt.Printf("airflowp: %v\n", airflowp)
		out = append(out, airflowp)
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


