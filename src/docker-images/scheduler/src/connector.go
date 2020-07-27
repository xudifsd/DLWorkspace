package main

import (
	"fmt"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	listerV1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog"
	"time"
)

// Connect scheduler to outside world, this abstruct to make test easier
type Connector interface {
	GetAllPods() (ret []*v1.Pod, err error)
	GetAllNodes() (ret []*v1.Node, err error)
	BindPodToNode(podNamespace, podName, nodeName string)
	CreatePodEvent(podNamespace, podName, message, reason string, podUID types.UID, timestamp time.Time)
}

type K8sConnector struct {
	clientset  *kubernetes.Clientset
	nodeLister listerV1.NodeLister
	podLister  listerV1.PodLister
}

func NewK8sConnector(clientset *kubernetes.Clientset, podLister listerV1.PodLister, nodeLister listerV1.NodeLister) *K8sConnector {
	return &K8sConnector{
		clientset:  clientset,
		nodeLister: nodeLister,
		podLister:  podLister,
	}
}

func (c K8sConnector) GetAllPods() ([]*v1.Pod, error) {
	return c.podLister.List(labels.Everything())
}

func (c K8sConnector) GetAllNodes() ([]*v1.Node, error) {
	return c.nodeLister.List(labels.Everything())
}

func (c K8sConnector) BindPodToNode(podNamespace, podName, nodeName string) {
	klog.Infof("bind pod %s to node %s",
		fmt.Sprintf("%s/%s", podNamespace, podName), nodeName)

	c.clientset.CoreV1().Pods(podNamespace).Bind(&v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: podNamespace,
		},
		Target: v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       nodeName,
		},
	})
}

func (c K8sConnector) CreatePodEvent(podNamespace, podName, message, reason string, podUID types.UID, timestamp time.Time) {
	c.clientset.CoreV1().Events(podNamespace).Create(&v1.Event{
		Count:          1,
		Message:        message,
		Reason:         reason,
		LastTimestamp:  metav1.NewTime(timestamp),
		FirstTimestamp: metav1.NewTime(timestamp),
		Type:           "Normal",
		Source: v1.EventSource{
			Component: SCHEDULER_NAME + " scheduler",
		},
		InvolvedObject: v1.ObjectReference{
			Kind:      "Pod",
			Name:      podName,
			Namespace: podNamespace,
			UID:       podUID,
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: podName + "-",
		},
	})
}
