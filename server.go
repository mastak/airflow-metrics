package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/mastak/airflow-metrics/metrics"
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
		airflowProcesses, err := metrics.AirflowPythonProcesses()
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

	if hostnamePath != "" {
		content, err := ioutil.ReadFile(hostnamePath)
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
