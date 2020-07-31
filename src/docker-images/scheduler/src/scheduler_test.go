package main

import (
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Bindings is pod key to node name
type ConnectorForTest struct {
	Bindings map[string]string
}

func NewConnectorForTest() *ConnectorForTest {
	return &ConnectorForTest{
		Bindings: make(map[string]string),
	}
}

func (c ConnectorForTest) GetAllPods() ([]*v1.Pod, error) {
	return make([]*v1.Pod, 0), nil
}

func (c ConnectorForTest) GetAllNodes() ([]*v1.Node, error) {
	return make([]*v1.Node, 0), nil
}

func (c ConnectorForTest) BindPodToNode(podNamespace, podName, nodeName string) {
	c.Bindings[fmt.Sprintf("%s/%s", podNamespace, podName)] = nodeName
}

func (c ConnectorForTest) CreatePodEvent(podNamespace, podName, message, reason string, podUID types.UID, timestamp time.Time) {
}

func (c ConnectorForTest) assertBindTo(t *testing.T, key, nodeName string) {
	if val, ok := c.Bindings[key]; ok {
		if val != nodeName {
			t.Errorf("bind %s to %s, expect %s", key, val, nodeName)
		}
	} else {
		t.Errorf("failed to bind %s", key)
	}
}

func (c ConnectorForTest) assertNotBound(t *testing.T, key string) {
	if val, ok := c.Bindings[key]; ok {
		t.Errorf("bind %s to %s, expect not bound", key, val)
	}
}

func (c ConnectorForTest) getBindingNode(t *testing.T, key string) string {
	if val, ok := c.Bindings[key]; ok {
		return val
	} else {
		t.Errorf("failed to bind %s", key)
		return ""
	}
}

func newNode(name string, labels map[string]string, cpu, memory, gpu int64, unschedulable bool) *v1.Node {
	capacity := make(map[v1.ResourceName]resource.Quantity)
	capacity[v1.ResourceCPU] = *resource.NewQuantity(cpu, "")
	capacity[v1.ResourceMemory] = *resource.NewQuantity(memory, "")
	capacity["nvidia.com/gpu"] = *resource.NewQuantity(gpu, "")

	allocatable := make(map[v1.ResourceName]resource.Quantity)
	allocatable[v1.ResourceCPU] = *resource.NewQuantity(cpu, "")
	allocatable[v1.ResourceMemory] = *resource.NewQuantity(memory, "")
	allocatable["nvidia.com/gpu"] = *resource.NewQuantity(gpu, "")

	result := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		}}
	result.Spec.Unschedulable = unschedulable
	result.Status.Capacity = capacity
	result.Status.Allocatable = allocatable

	return result
}

func newPod(nameSpace, name string, labels map[string]string, schedulerName string, nodeSelector map[string]string, cpu, memory, gpu int64) *v1.Pod {

	result := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: nameSpace,
			Name:      name,
			Labels:    labels,
		}}
	result.Spec.SchedulerName = schedulerName
	result.Spec.NodeSelector = nodeSelector

	container := v1.Container{
		Name: "main",
	}

	container.Resources.Requests = make(map[v1.ResourceName]resource.Quantity)
	container.Resources.Requests[v1.ResourceCPU] = *resource.NewQuantity(cpu, "")
	container.Resources.Requests[v1.ResourceMemory] = *resource.NewQuantity(memory, "")
	container.Resources.Requests["nvidia.com/gpu"] = *resource.NewQuantity(gpu, "")
	result.Spec.Containers = append(result.Spec.Containers, container)

	return result
}

func TestNormal(t *testing.T) {
	connector := NewConnectorForTest()
	scheduler := NewScheduler(connector)

	scheduler.UpdateNode("n1", newNode("n1", make(map[string]string), 10, 10, 10, false))
	scheduler.UpdatePod("default/p1",
		newPod("default", "p1", make(map[string]string), SCHEDULER_NAME, make(map[string]string), 1, 1, 1))
	scheduler.schedule()
	connector.assertBindTo(t, "default/p1", "n1")

	scheduler.UpdatePod("default/p2",
		newPod("default", "p2", make(map[string]string), SCHEDULER_NAME, make(map[string]string), 1, 1, 1))
	scheduler.schedule()
	connector.assertBindTo(t, "default/p2", "n1")

	scheduler.UpdatePod("default/p3",
		newPod("default", "p3", make(map[string]string), SCHEDULER_NAME, make(map[string]string), 50, 50, 50))
	scheduler.schedule()
	connector.assertNotBound(t, "default/p3")

	scheduler.UpdateNode("n2", newNode("n2", make(map[string]string), 100, 100, 100, false))
	scheduler.schedule()
	connector.assertBindTo(t, "default/p3", "n2")
}

func TestResourceNotEnough(t *testing.T) {
	connector := NewConnectorForTest()
	scheduler := NewScheduler(connector)

	scheduler.UpdateNode("n1", newNode("n1", make(map[string]string), 1, 1, 1, false))
	scheduler.UpdatePod("default/p1",
		newPod("default", "p1", make(map[string]string), SCHEDULER_NAME, make(map[string]string), 1, 1, 1))
	scheduler.schedule()
	connector.assertBindTo(t, "default/p1", "n1")

	scheduler.UpdatePod("default/p2",
		newPod("default", "p2", make(map[string]string), SCHEDULER_NAME, make(map[string]string), 1, 1, 1))

	scheduler.schedule()

	connector.assertNotBound(t, "default/p2")
}

func TestDoNotPickPodsOfOtherScheduler(t *testing.T) {
	connector := NewConnectorForTest()
	scheduler := NewScheduler(connector)

	scheduler.UpdateNode("n1", newNode("n1", make(map[string]string), 1, 1, 1, false))
	scheduler.UpdatePod("default/p1",
		newPod("default", "p1", make(map[string]string), "default", make(map[string]string), 1, 1, 2))
	scheduler.schedule()
	connector.assertNotBound(t, "default/p1")
}

func TestRespectNodeSelector(t *testing.T) {
	connector := NewConnectorForTest()
	scheduler := NewScheduler(connector)

	scheduler.UpdateNode("n1", newNode("n1", make(map[string]string), 1, 1, 1, false))
	nodeSelector := make(map[string]string)
	nodeSelector["foo"] = "bar"
	scheduler.UpdatePod("default/p1",
		newPod("default", "p1", make(map[string]string), "default", nodeSelector, 1, 1, 1))
	scheduler.schedule()
	connector.assertNotBound(t, "default/p1")
}

func TestRespectUnschedulable(t *testing.T) {
	connector := NewConnectorForTest()
	scheduler := NewScheduler(connector)

	scheduler.UpdateNode("n1", newNode("n1", make(map[string]string), 1, 1, 1, true))
	scheduler.UpdatePod("default/p1",
		newPod("default", "p1", make(map[string]string), "default", make(map[string]string), 1, 1, 1))
	scheduler.schedule()
	connector.assertNotBound(t, "default/p1")
}

func TestMinimizeGPUFragmentation(t *testing.T) {
	connector := NewConnectorForTest()
	scheduler := NewScheduler(connector)

	for i := 0; i < 2; i++ {
		nodeName := fmt.Sprintf("n%d", i)
		scheduler.UpdateNode(nodeName, newNode(nodeName, make(map[string]string), 5, 5, 5, false))
	}

	scheduler.UpdatePod("default/pivot",
		newPod("default", "pivot", make(map[string]string), SCHEDULER_NAME, make(map[string]string), 1, 1, 1))
	scheduler.schedule()
	pivotNode := connector.getBindingNode(t, "default/pivot")

	for i := 0; i < 4; i++ {
		podName := fmt.Sprintf("p%d", i)
		key := fmt.Sprintf("default/%s", podName)
		scheduler.UpdatePod(key,
			newPod("default", podName, make(map[string]string), SCHEDULER_NAME, make(map[string]string), 1, 1, 1))
	}
	scheduler.schedule()

	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("default/p%d", i)
		connector.assertBindTo(t, key, pivotNode)
	}
}
