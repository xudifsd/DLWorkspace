package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog"
)

const SCHEDULER_NAME = "DLTS"

func IsSelectorMatchs(labels, selector map[string]string) bool {
	for k, v := range selector {
		if val, ok := labels[k]; ok && val == v {
			continue
		}
		return false
	}
	return true
}

type Scheduler struct {
	connector Connector

	nodesMu sync.Mutex
	nodes   map[string]*NodeInfo

	podsMu sync.Mutex
	pods   map[string]*PodInfo
}

// Not concurrency safe object
type Resource struct {
	Cpu    int64
	Memory int64
	Gpu    int64
}

func (r *Resource) Update(another *Resource) {
	r.Cpu = another.Cpu
	r.Memory = another.Memory
	r.Gpu = another.Gpu
}

func (r *Resource) CanSatisfy(request *Resource) bool {
	return r.Cpu >= request.Cpu &&
		r.Memory >= request.Memory &&
		r.Gpu >= request.Gpu
}

func (r *Resource) Sub(request *Resource) {
	r.Cpu -= request.Cpu
	r.Memory -= request.Memory
	r.Gpu -= request.Gpu
}

// Not concurrency safe object, should hold node's lock when accessing this.
// This is cache of the current resource usage, Allocatable is reported by Node
// and Allocated is resources used by pods assigned to this node.
// We will add/delete Allocated whenever we see updates from k8s or we
// assigned/preempt pods ourself. Scheduling decision is made based on this cache.
type ResourceAllocator struct {
	// Same with allocatable in node spec. These don't count used resources
	Allocatable Resource

	Allocated map[string]*Resource
}

func NewAllocator() *ResourceAllocator {
	return &ResourceAllocator{Allocated: make(map[string]*Resource)}
}

func (a *ResourceAllocator) GetFreeResource() (result *Resource) {
	result = &Resource{}
	result.Update(&a.Allocatable)

	for _, v := range a.Allocated {
		result.Sub(v)
	}
	return
}

func (a *ResourceAllocator) Use(key string, used *Resource) {
	value := &Resource{}
	value.Update(used)

	a.Allocated[key] = used
}

func (a *ResourceAllocator) Free(key string) {
	delete(a.Allocated, key)
}

type NodeInfo struct {
	mu sync.Mutex

	Name   string // assume name can not be changed
	Labels map[string]string

	allocator *ResourceAllocator

	Unschedulable bool
}

func (n *NodeInfo) GetFreeResource() *Resource {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.allocator.GetFreeResource()
}

func NewNodeInfo(node *v1.Node) *NodeInfo {
	info := &NodeInfo{
		Name:      node.Name,
		Labels:    make(map[string]string),
		allocator: NewAllocator(),

		Unschedulable: node.Spec.Unschedulable,
	}

	for k, v := range node.Labels {
		info.Labels[k] = v
	}
	allocatable := node.Status.Allocatable
	if allocatable != nil {
		cpu, memory, gpu := int64(0), int64(0), int64(0)

		if allocatable.Cpu() != nil {
			cpu = allocatable.Cpu().Value()
		}
		if allocatable.Memory() != nil {
			memory = allocatable.Memory().Value()
		}
		if val, ok := allocatable["nvidia.com/gpu"]; ok {
			gpu = val.Value()
		}
		info.allocator.Allocatable.Update(&Resource{Cpu: cpu, Memory: memory, Gpu: gpu})
	}

	return info
}

func (n *NodeInfo) Update(another *NodeInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Labels = make(map[string]string)
	for k, v := range another.Labels {
		n.Labels[k] = v
	}

	n.allocator.Allocatable.Update(&another.allocator.Allocatable)
}

func (n *NodeInfo) Use(key string, request *Resource) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.allocator.Use(key, request)
}

func (n *NodeInfo) Free(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.allocator.Free(key)
}

func (n *NodeInfo) String() string {
	n.mu.Lock()
	defer n.mu.Unlock()

	free := n.allocator.GetFreeResource()

	unschedulable := "f"
	if n.Unschedulable {
		unschedulable = "t"
	}

	return fmt.Sprintf("%s[%s](c:%d,g:%d)", n.Name, unschedulable, free.Cpu, free.Gpu)
}

type PodInfo struct {
	mu sync.Mutex

	Key       string
	Name      string
	Namespace string
	UID       types.UID

	VcName   string
	UserName string
	JobId    string

	// will be empty if not scheduled
	NodeName string

	SchedulerName string

	NodeSelector map[string]string

	PreemptionAllowed bool

	// required resources
	RequiredResource Resource
}

func NewPodInfo(key string, pod *v1.Pod) *PodInfo {
	vcName := "unknown"
	userName := "unknown"
	jobId := "unknown"
	ok := true

	warningMsg := make([]string, 0)

	if vcName, ok = pod.Labels["vcName"]; !ok {
		warningMsg = append(warningMsg, "unknown vc name")
	}
	if userName, ok = pod.Labels["userName"]; !ok {
		warningMsg = append(warningMsg, "unknown user name")
	}
	if jobId, ok = pod.Labels["jobId"]; !ok {
		warningMsg = append(warningMsg, "unknown job id")
	}
	preemptionAllowedS, ok := pod.Labels["preemptionAllowed"]
	if !ok {
		preemptionAllowedS = "False"
	}
	preemptionAllowed, err := strconv.ParseBool(preemptionAllowedS)
	if err != nil {
		warningMsg = append(warningMsg, fmt.Sprintf("failed to convert preemptionAllowed %s to bool, err %v",
			preemptionAllowedS, err))
	}
	if len(warningMsg) > 0 {
		klog.Warningf("%s: %s", key, strings.Join(warningMsg, ", "))
	}

	info := &PodInfo{
		Key:       key,
		Name:      pod.Name,
		Namespace: pod.Namespace,
		UID:       pod.UID,

		VcName:   vcName,
		UserName: userName,
		JobId:    jobId,
		NodeName: pod.Spec.NodeName,

		SchedulerName: pod.Spec.SchedulerName,

		NodeSelector: make(map[string]string),

		PreemptionAllowed: preemptionAllowed,
	}

	for k, v := range pod.Spec.NodeSelector {
		info.NodeSelector[k] = v
	}

	for _, container := range pod.Spec.Containers {
		requests := container.Resources.Requests
		if requests != nil {
			cpu, memory, gpu := int64(0), int64(0), int64(0)
			if requests.Cpu() != nil {
				cpu = requests.Cpu().Value()
			}
			if requests.Memory() != nil {
				memory = requests.Memory().Value()
			}
			if val, ok := requests["nvidia.com/gpu"]; ok {
				gpu = val.Value()
			}
			info.RequiredResource.Update(&Resource{Cpu: cpu, Memory: memory, Gpu: gpu})
		}
	}

	return info
}

func (p *PodInfo) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return fmt.Sprintf("%s(c:%d,g:%d)", p.Key, p.RequiredResource.Cpu, p.RequiredResource.Gpu)
}

func NewScheduler(connector Connector) *Scheduler {
	return &Scheduler{
		connector: connector,
		nodes:     make(map[string]*NodeInfo),
		pods:      make(map[string]*PodInfo),
	}
}

func (s *Scheduler) fullSync() {
	nodes, err := s.connector.GetAllNodes()
	if err != nil {
		panic(err.Error())
	}

	for _, node := range nodes {
		err := s.UpdateNode(node.Name, node)
		if err != nil {
			panic(err.Error())
		}
	}

	pods, err := s.connector.GetAllPods()
	if err != nil {
		panic(err.Error())
	}

	for _, pod := range pods {
		key := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
		err := s.UpdatePod(key, pod)
		if err != nil {
			panic(err.Error())
		}
	}
}

func (s *Scheduler) Run(stopCh <-chan struct{}) {
	s.fullSync()

	go wait.Until(s.schedule, 10*time.Second, stopCh)
	<-stopCh
}

// should treat pod as readonly
func (s *Scheduler) UpdatePod(key string, pod *v1.Pod) (returnedErr error) {
	klog.Infof("update pod %s in scheduler", key)
	s.podsMu.Lock()
	defer s.podsMu.Unlock()

	if oldPod, found := s.pods[key]; found {
		if oldPod.NodeName != "" {
			s.nodesMu.Lock()
			if oldNode, found := s.nodes[oldPod.NodeName]; found {
				oldNode.Free(oldPod.Key)
			}
			s.nodesMu.Unlock()
		}
	}

	podInfo := NewPodInfo(key, pod)
	s.pods[key] = podInfo
	if podInfo.NodeName != "" {
		s.nodesMu.Lock()
		if node, found := s.nodes[podInfo.NodeName]; found {
			node.Use(podInfo.Key, &podInfo.RequiredResource)
		}
		s.nodesMu.Unlock()
	}
	return
}

func (s *Scheduler) DeletePod(key, namespace, name string) (returnedErr error) {
	klog.Infof("delete pod %s from scheduler", key)
	s.podsMu.Lock()
	defer s.podsMu.Unlock()

	if oldPod, found := s.pods[key]; found {
		if oldPod.NodeName != "" {
			s.nodesMu.Lock()
			if oldNode, found := s.nodes[oldPod.NodeName]; found {
				oldNode.Free(oldPod.Key)
			}
			s.nodesMu.Unlock()
		}
	}

	delete(s.pods, key)
	return
}

// should treat node as readonly
func (s *Scheduler) UpdateNode(name string, node *v1.Node) (returnedErr error) {
	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()

	oldNode := s.nodes[name]
	newNode := NewNodeInfo(node)

	if oldNode == nil {
		klog.Infof("add node %s to scheduler", newNode.String())
		s.nodes[name] = newNode
	} else {
		klog.Infof("update node %s -> %s in scheduler", oldNode.String(), newNode.String())
		oldNode.Update(newNode)
	}

	return
}

func (s *Scheduler) DeleteNode(name string) (returnedErr error) {
	klog.Infof("delete node %s from scheduler", name)
	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()

	delete(s.nodes, name)
	return
}

func (s *Scheduler) bindPodToNode(pod *PodInfo, node *NodeInfo) {
	pod.NodeName = node.Name
	s.connector.BindPodToNode(pod.Namespace, pod.Name, node.Name)

	timestamp := time.Now().UTC()

	s.connector.CreatePodEvent(pod.Namespace, pod.Name, "Successfully scheduled", "Scheduled", pod.UID, timestamp)
}

func (s *Scheduler) scheduleFailed(pod *PodInfo, counter *SchedulerCounter) {
	timestamp := time.Now().UTC()

	message := fmt.Sprintf("Failed to schedule: %d resource not enough nodes, %d selector mismatch nodes, unschedulable %d",
		counter.ResourceNotEnought, counter.SelectorNotMatch, counter.Unschedulable)

	s.connector.CreatePodEvent(pod.Namespace, pod.Name, message, "Failed to schedule", pod.UID, timestamp)
}

type SchedulerCounter struct {
	ResourceNotEnought int
	SelectorNotMatch   int
	Unschedulable      int
}

func (c *SchedulerCounter) String() string {
	return fmt.Sprintf("r:%d,s:%d,u:%d", c.ResourceNotEnought, c.SelectorNotMatch, c.Unschedulable)
}

type CachedNode struct {
	FreeResource *Resource
	Node         *NodeInfo
}

func newCachedNode(node *NodeInfo) *CachedNode {
	return &CachedNode{
		FreeResource: node.GetFreeResource(),
		Node:         node,
	}
}

func (c *CachedNode) Use(key string, used *Resource) {
	c.FreeResource.Sub(used)
	c.Node.Use(key, used)
}

func (c *CachedNode) String() string {
	return fmt.Sprintf("%s{c:%d,g%d}", c.Node.String(), c.FreeResource.Cpu, c.FreeResource.Gpu)
}

func sortCachedNodes(target []*CachedNode) {
	sort.SliceStable(target, func(i, j int) bool {
		ii := target[i].FreeResource
		jj := target[j].FreeResource

		if ii.Gpu < jj.Gpu {
			return true
		} else if ii.Gpu > jj.Gpu {
			return false
		} else {
			if ii.Cpu < jj.Cpu {
				return true
			} else if ii.Cpu > jj.Cpu {
				return false
			} else {
				if ii.Memory < jj.Memory {
					return true
				} else if ii.Memory > jj.Memory {
					return false
				} else {
					return strings.Compare(target[i].Node.Name, target[j].Node.Name) <= 0
				}
			}
		}
	})
}

// whenever want to lock pods and nodes, should always lock pods first to avoid deadlock
func (s *Scheduler) schedule() {
	startTime := time.Now()
	defer func() {
		klog.Infof("spent %v in one scheduling pass", time.Since(startTime))
	}()

	s.podsMu.Lock()
	s.nodesMu.Lock()
	defer s.podsMu.Unlock()
	defer s.nodesMu.Unlock()

	podsInfo := make([]string, 0)
	for _, pod := range s.pods {
		if pod.SchedulerName == SCHEDULER_NAME {
			podsInfo = append(podsInfo, pod.String())
		}
	}

	nodesInfo := make([]string, 0)

	cachedNodes := make([]*CachedNode, 0)
	for _, node := range s.nodes {
		cachedNodes = append(cachedNodes, newCachedNode(node))
	}
	sortCachedNodes(cachedNodes)

	nodesInfo = make([]string, 0)
	for _, node := range cachedNodes {
		nodesInfo = append(nodesInfo, node.String())
	}

	klog.Infof("start scheduling %v to %v",
		strings.Join(podsInfo, "|"), strings.Join(nodesInfo, "|"))

	for _, pod := range s.pods {
		if pod.NodeName != "" || pod.SchedulerName != SCHEDULER_NAME {
			continue
		}
		scheduled := false
		schedulerCounter := &SchedulerCounter{}

		for _, cached := range cachedNodes {
			if cached.Node.Unschedulable {
				schedulerCounter.Unschedulable += 1
				continue
			}

			if !IsSelectorMatchs(cached.Node.Labels, pod.NodeSelector) {
				schedulerCounter.SelectorNotMatch += 1
				continue
			}

			if !cached.FreeResource.CanSatisfy(&pod.RequiredResource) {
				schedulerCounter.ResourceNotEnought += 1
				continue
			}

			cached.Use(pod.Key, &pod.RequiredResource)
			sortCachedNodes(cachedNodes)

			s.bindPodToNode(pod, cached.Node)
			scheduled = true

			break
		}

		if !scheduled {
			s.scheduleFailed(pod, schedulerCounter)
			klog.Infof("failed to schedule %s in one pass: %s",
				pod.Key, schedulerCounter)
		}
	}
}
