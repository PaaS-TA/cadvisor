// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package influxdb

import (
	"os"
	"sync"
	"time"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"errors"
	"strings"
	"net"

	"github.com/golang/glog"
	"github.com/google/cadvisor/storage"
	info "github.com/google/cadvisor/info/v1"
	influxdb "github.com/influxdata/influxdb/client/v2"
	"fmt"
)

func init() {
	storage.RegisterStorageDriver("influxdb", new)
}

type influxdbStorage struct {
	client          influxdb.Client
	cellIp 		string
	machineName     string
	database        string
	retentionPolicy string
	bufferDuration  time.Duration
	lastWrite       time.Time
	points          []*influxdb.Point
	lock            sync.Mutex
	readyToFlush    func() bool
}

//====================================================================================
// Container Metrics Metadata from REP (127.0.0.1:1800/v1/containers)
type ContainerMetricsMetadata struct{
	Limits 			Limits		`json:"limits,omitempty"`
	UsageMetrics		UsageMetrics 	`json:"usage_metrics,omitempty"`
	Container_Id 		string		`json:"container_id,omitempty"`
	Interface_Id 		string 		`json:"interface_id,omitempty"`
	Application_Id 		string		`json:"application_id,omitempty"`
	Application_Index 	string 		`json:"application_index,omitempty"`
	Application_Name 	string		`json:"application_name,omitempty"`
	Application_Urls 	[]string	`json:"application_uris,omitempty"`
}

type Limits struct {
	Fds    int32        `json:"fds,omitempty"`
	Memory int32        `json:"mem,omitempty"`
	Disk   int32        `json:"disk,omitempty"`
}

type UsageMetrics struct {
	MemoryUsageInBytes uint64        `json:"memory_usage_in_bytes"`
	DiskUsageInBytes   uint64        `json:"disk_usage_in_bytes"`
	TimeSpentInCPU     time.Duration `json:"time_spent_in_cpu"`
}
//====================================================================================

// Series names
const (
	// Cumulative CPU usage
	serCpuUsageTotal  string = "cpu_usage_total"
	serCpuUsageSystem string = "cpu_usage_system"
	serCpuUsageUser   string = "cpu_usage_user"
	serCpuUsagePerCpu string = "cpu_usage_per_cpu"
	// Smoothed average of number of runnable threads x 1000.
	serLoadAverage string = "load_average"
	// Memory Usage
	serMemoryUsage string = "memory_usage"
	// Working set size
	serMemoryWorkingSet string = "memory_working_set"
	// Cumulative count of bytes received.
	serRxBytes string = "rx_bytes"
	// Cumulative count of receive errors encountered.
	serRxErrors string = "rx_errors"
	// Cumulative count of bytes transmitted.
	serTxBytes string = "tx_bytes"
	// Cumulative count of transmit errors encountered.
	serTxErrors string = "tx_errors"

	serRxDropped string = "rx_dropped"
	serTxDropped string = "tx_dropped"
	// Filesystem device.
	//serFsDevice string = "fs_device"
	// Filesystem limit.
	//serFsLimit string = "fs_limit"
	// Filesystem usage.
	//serFsUsage string = "fs_usage"

	// Disk Usage
	serDiskUsage string = "disk_usage"

	// Container Measurement
	serContainerMeausement string = "container_metrics"
)

func new() (storage.StorageDriver, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}



	//=================================================================================
	// VM IP address
	var instanceIp string
	ifaces, err := net.Interfaces()
	if err != nil{
		instanceIp = ""
	}
	for _, iface := range ifaces{
		//fmt.Println("##### local network interface name :", iface.Name)
		if strings.HasPrefix(iface.Name, "en") || strings.HasPrefix(iface.Name, "eth"){
			addrs, _ := iface.Addrs()
			for _, addr := range addrs {
				//Check whether addr is  IP adress or Mac address.
				ip_array := strings.Split(addr.String(), ".")
				if len(ip_array) >= 4{
					//var ip net.IP
					switch v := addr.(type) {
					case *net.IPNet:
						instanceIp = v.IP.String()
					}
				}
			}
		}
	}
	//fmt.Println("##### local network address 1 :", instanceIp)
	if instanceIp == "" {
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			instanceIp = ""
		}
		for _, address := range addrs {
			// check the address type and if it is not a loopback the display it
			if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					instanceIp = ipnet.IP.String() //fmt.Println(ipnet.IP.String())
				}

			}
		}
	}
	fmt.Println("##### local network address 2 :", instanceIp)
	//=================================================================================
	/*
	var cellIp string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {

			fmt.Println("address :", address.Network())
			fmt.Println("ipnet :", ipnet.IP)

			if ipnet.IP.To4() != nil {
				cellIp = ipnet.IP.String()
				fmt.Println("cellIp:", ipnet.IP.String())
			}

		}
	}*/

	return newStorage(
		hostname,
		instanceIp,
		*storage.ArgDbTable,
		*storage.ArgDbName,
		*storage.ArgDbHost,
	)
}

// machineName: A unique identifier to identify the host that current cAdvisor
// instance is running on.
// influxdbHost: The host which runs influxdb (host:port)
func newStorage(
machineName,
cellIp,
tablename,
database,
influxdbHost string,
) (*influxdbStorage, error) {

	// Make client
	client, err := influxdb.NewUDPClient(influxdb.UDPConfig{
		Addr: influxdbHost,
		//PayloadSize: 4096,
	})

	if err != nil {
		return nil, err
	}

	ret := &influxdbStorage{
		client:         client,
		machineName:    machineName,
		cellIp: 	cellIp,
		database:       database,
		lastWrite:      time.Now(),
		points:         make([]*influxdb.Point, 0),
	}
	ret.readyToFlush = ret.defaultReadyToFlush
	return ret, nil
}

// Field names
const (
	fieldAppDisk string = "app_disk"
	fieldAppMem  string = "app_mem"
	fieldValue   string = "value"
	fieldType    string = "type"
	fieldDevice  string = "device"
)

// Tag names
const (
	tagName		 	string = "name"
	tagMachineName   	string = "machine"
	tagContainerName 	string = "container_id"
	tagContainerInterface  	string = "container_interface"
	tagCellIp 	 	string = "cell_ip"
	tagApplicationId 	string = "application_id"
	tagApplicationIndex 	string = "application_index"
	tagApplicationName 	string = "application_name"
	tagApplicationUrl 	string = "application_url"
)
//====================================================================================
// Container Metrics Metadata from REP (127.0.0.1:1800/v1/containers)
func (self *influxdbStorage) containerMetricsMedataData() []ContainerMetricsMetadata{
	client := &http.Client{
		CheckRedirect: func(req *http.Request, _ []*http.Request) error {
			//dumpRequest(req)
			return errors.New("No redirects")
		},
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives:   true,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}

	reqUrl := "http://127.0.0.1:1800/v1/containers"
	req, err := http.NewRequest("GET", reqUrl, nil)
	resp, err := client.Do(req)
	if err != nil {
		glog.Error("##### get Container Metrics Metadata request err:", err)
	}
	if resp != nil{
		rawdata, _ := ioutil.ReadAll(resp.Body)
		//fmt.Println("##### Response Data :", string(rawdata))

		var containermetrics []ContainerMetricsMetadata
		json.Unmarshal(rawdata, &containermetrics)

		/*fmt.Println("##### Container Metrics Metadata :", containermetrics, len(containermetrics))
		for _, metrics :=range containermetrics{
			fmt.Println("##### Container Metrics container id :", metrics.Container_Id)
			fmt.Println("##### Container Metrics app id :", metrics.Application_Id)
			fmt.Println("##### Container Metrics app name :", metrics.Application_Name)
			fmt.Println("##### Container Metrics app urls :", metrics.Application_Urls)
			fmt.Println("##### Container Metrics app limits :", metrics.Limits)
			fmt.Println("##### Container Metrics app usage-memory :", metrics.UsageMetrics.MemoryUsageInBytes)
			fmt.Println("##### Container Metrics app usage-disk :", metrics.UsageMetrics.DiskUsageInBytes)
			fmt.Println("##### Container Metrics app usage-cpu(second) :", metrics.UsageMetrics.TimeSpentInCPU.Seconds())
		}*/
		return containermetrics
	}
	return nil
}
//====================================================================================

func (self *influxdbStorage) containerFilesystemStatsToPoints(
	//ref info.ContainerReference,
	containerName string,
	containerInterface string,
	stats *info.ContainerStats) (points []*influxdb.Point) {
	if len(stats.Filesystem) == 0 {
		return points
	}

	for _, fsStat := range stats.Filesystem {
		tagsFsUsage := map[string]string{
			tagMachineName: self.machineName,
			tagContainerName: containerName,
			tagContainerInterface: containerInterface,
			fieldDevice: fsStat.Device,
			fieldType:   "usage",
		}
		fieldsFsUsage := map[string]interface{}{
			fieldValue: float64(fsStat.Usage),
		}

		fsUsagePt, err :=influxdb.NewPoint(serContainerMeausement, tagsFsUsage, fieldsFsUsage)
		if err != nil {
			glog.Fatalf("Failed to create NewPoint for FieldsFsUsage: %v", err)
		}
		tagsFsLimit := map[string]string{
			tagMachineName: self.machineName,
			tagContainerName: containerName,
			tagContainerInterface: containerInterface,
			fieldDevice: fsStat.Device,
			fieldType:   "limit",
		}
		fieldsFsLimit := map[string]interface{}{
			fieldValue: float64(fsStat.Limit),
		}
		fsLimitPt, err := influxdb.NewPoint(serContainerMeausement, tagsFsLimit, fieldsFsLimit)
		if err != nil {
			glog.Fatalf("Failed to create NewPoint for FieldsFsLimit: %v", err)
		}
		points = append(points, fsUsagePt, fsLimitPt)
	}
	//self.tagPoints(ref, stats, points)
	return points
}

func (self *influxdbStorage) containerStatsToPoints(
	//ref info.ContainerReference,
	containerName string,
	containerMetric ContainerMetricsMetadata,
	stats *info.ContainerStats,
) (points []*influxdb.Point) {

	/*
	//============================= rep (/v1/containers)로부터 container resource usage metrics 정보를 전달받아 influxdb에 저장한다. ============================
	// rep로부터 전달받는 mertrics : cpu, memory, disk usage

	// CPU usage: Total usage in nanoseconds
	points = append(points, makePoint(self.machineName, self.cellIp, containerName, serCpuUsageTotal, containerMetric, float64(stats.Cpu.Usage.Total)))

	// CPU usage: Time spend in system space (in nanoseconds)
	points = append(points, makePoint(self.machineName, self.cellIp, containerName, serCpuUsageSystem, containerMetric, float64(stats.Cpu.Usage.System)))

	// CPU usage: Time spent in user space (in nanoseconds)
	points = append(points, makePoint(self.machineName, self.cellIp, containerName, serCpuUsageUser, containerMetric, float64(stats.Cpu.Usage.User)))

	// CPU usage per CPU
	for i := 0; i < len(stats.Cpu.Usage.PerCpu); i++ {
		point := makePoint(self.machineName, self.cellIp, containerName, serCpuUsagePerCpu, containerMetric, float64(stats.Cpu.Usage.PerCpu[i]))
		*//*tags := map[string]string{"instance": fmt.Sprintf("%v", i)}
		addTagsToPoint(point, tags)*//*

		points = append(points, point)
	}

	// Load Average
	points = append(points, makePoint(self.machineName, self.cellIp, containerName, serLoadAverage, containerMetric, float64(stats.Cpu.LoadAverage)))

	// Memory Usage
	points = append(points, makePoint(self.machineName, self.cellIp, containerName, serMemoryUsage, containerMetric, float64(stats.Memory.Usage)))

	// Working Set Size
	points = append(points, makePoint(self.machineName, self.cellIp, containerName, serMemoryWorkingSet, containerMetric, float64(stats.Memory.WorkingSet)))
	*/

	// CPU Usage
	points = append(points, makePoint(self.machineName, self.cellIp, containerName, serCpuUsageTotal, containerMetric, containerMetric.UsageMetrics.TimeSpentInCPU.Seconds()))
	// Load Average
	points = append(points, makePoint(self.machineName, self.cellIp, containerName, serLoadAverage, containerMetric, float64(stats.Cpu.LoadAverage)))
	// Memory Usage
	points = append(points, makePoint(self.machineName, self.cellIp, containerName, serMemoryUsage, containerMetric, float64(containerMetric.UsageMetrics.MemoryUsageInBytes)))
	// Disk Usage
	points = append(points, makePoint(self.machineName, self.cellIp, containerName, serDiskUsage, containerMetric, float64(containerMetric.UsageMetrics.DiskUsageInBytes)))

	// Network Stats
	for i := 0 ; i < len(stats.Network.Interfaces); i ++ {
		/*fmt.Println("interface name :", stats.Network.Interfaces[i].Name)
		fmt.Println("rxbytes :", stats.Network.Interfaces[i].RxBytes)
		fmt.Println("rxerror :", stats.Network.Interfaces[i].RxErrors)
		fmt.Println("rxdropped :", stats.Network.Interfaces[i].RxDropped)
		fmt.Println("txbytes :", stats.Network.Interfaces[i].TxBytes)
		fmt.Println("txerror :", stats.Network.Interfaces[i].TxErrors)
		fmt.Println("txdropped :", stats.Network.Interfaces[i].TxDropped)*/
		points = append(points, makePoint(self.machineName, self.cellIp, stats.Network.Interfaces[i].Name, serRxBytes, containerMetric, float64(stats.Network.Interfaces[i].RxBytes)))
		points = append(points, makePoint(self.machineName, self.cellIp, stats.Network.Interfaces[i].Name, serRxErrors, containerMetric, float64(stats.Network.Interfaces[i].RxErrors)))
		points = append(points, makePoint(self.machineName, self.cellIp, stats.Network.Interfaces[i].Name, serRxDropped, containerMetric, float64(stats.Network.Interfaces[i].RxDropped)))
		points = append(points, makePoint(self.machineName, self.cellIp, stats.Network.Interfaces[i].Name, serTxBytes, containerMetric, float64(stats.Network.Interfaces[i].TxBytes)))
		points = append(points, makePoint(self.machineName, self.cellIp, stats.Network.Interfaces[i].Name, serTxErrors, containerMetric, float64(stats.Network.Interfaces[i].TxErrors)))
		points = append(points, makePoint(self.machineName, self.cellIp, stats.Network.Interfaces[i].Name, serTxDropped, containerMetric, float64(stats.Network.Interfaces[i].TxDropped)))
	}



	//self.tagPoints(ref, stats, points)

	return points
}

func (self *influxdbStorage) OverrideReadyToFlush(readyToFlush func() bool) {
	self.readyToFlush = readyToFlush
}

func (self *influxdbStorage) defaultReadyToFlush() bool {
	return time.Since(self.lastWrite) >= self.bufferDuration
}

func (self *influxdbStorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {
	//fmt.Println("##### influxdb.go - AddStats called #####")
	if stats == nil {
		return nil
	}
	var pointsToFlush []*influxdb.Point
	func() {
		// AddStats will be invoked simultaneously from multiple threads and only one of them will perform a write.
		self.lock.Lock()
		defer self.lock.Unlock()

		var containerName, containerInterface string
		var containerMetric ContainerMetricsMetadata
		if len(ref.Aliases) > 0 {
			containerName = ref.Aliases[0]
		} else {
			containerName = ref.Name
		}
		//fmt.Println("================ containerName :", containerName)

		//===================================================================
		// Container Metrics Metadata from REP (127.0.0.1:1800/v1/containers)
		containerMetrics := self.containerMetricsMedataData()
		containerNames := strings.Split(containerName, "-")
		containerMetric.Container_Id = containerNames[len(containerNames) -1]
		for _, metrics := range containerMetrics{
			//fmt.Println("================ metrics:", metrics)
			//fmt.Println("================ metrics-contaier-id:", metrics.Container_Id)
			//fmt.Println("================ metrics-contaier-interface-id:", metrics.Interface_Id)

			//CAdvisor 0.23 버전과의 차이
			metric_container_id_array := strings.Split(metrics.Container_Id, "-")
			if len(metric_container_id_array) > 4 {
				//if metrics.Container_Id == containerMetric.Container_Id {
				if metric_container_id_array[len(metric_container_id_array)-1] == containerMetric.Container_Id {
					//fmt.Println("================ container id:", containerMetric.Container_Id)
					containerMetric.Interface_Id = metrics.Interface_Id
					containerMetric.Application_Id = metrics.Application_Id
					containerMetric.Application_Name = metrics.Application_Name
					containerMetric.Application_Urls = metrics.Application_Urls
					containerMetric.Application_Index = metrics.Application_Index
					containerMetric.Limits.Disk = metrics.Limits.Disk
					containerMetric.Limits.Memory = metrics.Limits.Memory
					containerMetric.UsageMetrics.MemoryUsageInBytes = metrics.UsageMetrics.MemoryUsageInBytes
					containerMetric.UsageMetrics.DiskUsageInBytes = metrics.UsageMetrics.DiskUsageInBytes
					containerMetric.UsageMetrics.TimeSpentInCPU = metrics.UsageMetrics.TimeSpentInCPU
				}
			}else{
				fmt.Println("================ else container id:", containerMetric.Container_Id, metrics.Container_Id)
			}
		}
		//===================================================================

		self.points = append(self.points, self.containerStatsToPoints(containerName, containerMetric, stats)...)
		self.points = append(self.points, self.containerFilesystemStatsToPoints(containerName, containerInterface, stats)...)
		if self.readyToFlush() {
			pointsToFlush = self.points
			self.points = make([]*influxdb.Point, 0)
			self.lastWrite = time.Now()

		}
	}()
	if len(pointsToFlush) > 0 {
		// Create a new point batch
		bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
			Database:  self.database,
			Precision: "s",
		})
		if err != nil {
			glog.Fatalf("Failed to create NewBatchPoint: %v", err)
		}

		//points := make([]influxdb.Point, len(pointsToFlush))
		for _, p := range pointsToFlush {
			//points[i] = *p
			//fmt.Println("point to save at database ",self.database, i, p)
			bp.AddPoint(p)
		}
		err = self.client.Write(bp)
		if err != nil {
			glog.Fatalf("Failed to send point to influxdb: %v", err)
		}
	}
	return nil
}

func (self *influxdbStorage) Close() error {
	self.client = nil
	return nil
}



// Creates a measurement point with a single value field
func makePoint(machineName, cellIp, containerName, name string, containerMetric ContainerMetricsMetadata, value float64) *influxdb.Point {
	var tags map[string]string
	var fields map[string]interface{}
	if containerMetric.Application_Id != "" {
		tags = map[string]string{
			tagName: name,
			tagMachineName: machineName,
			tagCellIp: cellIp,
			tagContainerName: containerName,
			tagContainerInterface: containerMetric.Interface_Id,
			tagApplicationId: containerMetric.Application_Id,
			tagApplicationIndex: containerMetric.Application_Index,
			tagApplicationName: containerMetric.Application_Name,
			tagApplicationUrl: containerMetric.Application_Urls[0],
		}
	}else{
		tags = map[string]string{
			tagName: name,
			tagMachineName: machineName,
			tagCellIp: cellIp,
			tagContainerName : containerName,
			tagContainerInterface : containerMetric.Interface_Id,
		}

	}
	if containerMetric.Application_Id != "" {
		fields = map[string]interface{}{
			fieldValue: value,
			fieldAppDisk: float64(containerMetric.Limits.Disk*1024*1024),
			fieldAppMem: float64(containerMetric.Limits.Memory*1024*1024),
		}
	}else{
		fields = map[string]interface{}{
			fieldValue: value,
		}
	}

	mkPoint, err := influxdb.NewPoint(serContainerMeausement, tags, fields)
	if err != nil {
		glog.Fatalf("Failed to create NewPoint for FieldsFsLimit: %v", err)
	}

	return mkPoint
}


// Some stats have type unsigned integer, but the InfluxDB client accepts only signed integers.
func toSignedIfUnsigned(value interface{}) interface{} {
	switch v := value.(type) {
	case uint64:
		return int64(v)
	case uint32:
		return int32(v)
	case uint16:
		return int16(v)
	case uint8:
		return int8(v)
	case uint:
		return int(v)
	}
	return value
}
