/*
Copyright 2022 The KubeEdge Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handler

import (
	"time"

	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/common"
	"github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/common/model"
	"github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/dispatcher"
	"github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/session"
	reliableclient "github.com/kubeedge/kubeedge/pkg/client/clientset/versioned"
	"github.com/kubeedge/viaduct/pkg/conn"
	"github.com/kubeedge/viaduct/pkg/mux"
)

type Handler interface {
	// HandleConnection is invoked when a new connection arrives
	HandleConnection(connection conn.Connection)

	// HandleMessage is invoked when a new message arrives.
	HandleMessage(container *mux.MessageContainer, writer mux.ResponseWriter)

	// note: OnEdgeNodeConnect和OnEdgeNodeDisconnect都是在HandleConnection中被调用

	// OnEdgeNodeConnect is invoked when a new connection is established
	OnEdgeNodeConnect(info *model.HubInfo, connection conn.Connection) error

	// OnEdgeNodeDisconnect is invoked when a connection is lost
	OnEdgeNodeDisconnect(info *model.HubInfo, connection conn.Connection)

	// OnReadTransportErr is invoked when the connection read message err
	OnReadTransportErr(nodeID, projectID string)
}

func NewMessageHandler(
	KeepaliveInterval int,
	manager *session.Manager,
	// 这里只用了reliableclient.Interface，实际的客户端对象是client.GetCRDClient()(github.com/kubeedge/kubeedge/cloud/pkg/common/client/client.go)
	// 这个client是K8s的client
	reliableClient reliableclient.Interface,
	dispatcher dispatcher.MessageDispatcher) Handler {
	messageHandler := &messageHandler{
		KeepaliveInterval: KeepaliveInterval,
		SessionManager:    manager,
		MessageDispatcher: dispatcher,
		reliableClient:    reliableClient,
	}

	// init handler that process upstream message
	messageHandler.initServerEntries()

	return messageHandler
}

// messageHandler是Handler的实现
type messageHandler struct {
	KeepaliveInterval int // 心跳检测间隔

	// SessionManager
	SessionManager *session.Manager

	// MessageDispatcher
	MessageDispatcher dispatcher.MessageDispatcher

	// reliableClient
	reliableClient reliableclient.Interface
}

// initServerEntries register handler func
func (mh *messageHandler) initServerEntries() {
	// note: mh.HandleMessage will be called in github.com/kubeedge/viaduct/pkg/mux/mux.go/dispatch/entry.handleFunc(container, writer)
	// 可以从这里一层层进去看看如何执行到entry.handleFunc(container, writer)的
	mux.Entry(mux.NewPattern("*").Op("*"), mh.HandleMessage)
}

// HandleMessage handle all the request from node
func (mh *messageHandler) HandleMessage(container *mux.MessageContainer, writer mux.ResponseWriter) {
	nodeID := container.Header.Get("node_id")
	projectID := container.Header.Get("project_id")

	// validate message
	if container.Message == nil {
		klog.Errorf("The message is nil for node: %s", nodeID)
		return
	}

	klog.V(4).Infof("[messageHandler]get msg from node(%s): %+v", nodeID, container.Message)

	// dispatch upstream message
	mh.MessageDispatcher.DispatchUpstream(container.Message, &model.HubInfo{ProjectID: projectID, NodeID: nodeID})
}

// HandleConnection is invoked when a new connection is established
func (mh *messageHandler) HandleConnection(connection conn.Connection) {
	nodeID := connection.ConnectionState().Headers.Get("node_id")
	projectID := connection.ConnectionState().Headers.Get("project_id")

	if mh.SessionManager.ReachLimit() {
		klog.Errorf("Fail to serve node %s, reach node limit", nodeID)
		return
	}

	nodeInfo := &model.HubInfo{ProjectID: projectID, NodeID: nodeID}

	if err := mh.OnEdgeNodeConnect(nodeInfo, connection); err != nil {
		klog.Errorf("publish connect event for node %s, err %v", nodeInfo.NodeID, err)
		return
	}

	// start a goroutine for serving the node connection
	go func() {
		klog.Infof("edge node %s for project %s connected", nodeInfo.NodeID, nodeInfo.ProjectID)

		// init node message pool and add to the dispatcher
		nodeMessagePool := common.InitNodeMessagePool(nodeID)
		mh.MessageDispatcher.AddNodeMessagePool(nodeID, nodeMessagePool)

		keepaliveInterval := time.Duration(mh.KeepaliveInterval) * time.Second
		// create a node session for each edge node
		nodeSession := session.NewNodeSession(nodeID, projectID, connection,
			keepaliveInterval, nodeMessagePool, mh.reliableClient)
		// add node session to the session manager
		mh.SessionManager.AddSession(nodeSession)

		// start session for each edge node and it will keep running until
		// it encounters some Transport Error from underlying connection.
		nodeSession.Start()

		klog.Infof("edge node %s for project %s disConnected", nodeInfo.NodeID, nodeInfo.ProjectID)

		// clean node message pool and session
		mh.MessageDispatcher.DeleteNodeMessagePool(nodeInfo.NodeID, nodeMessagePool)
		mh.SessionManager.DeleteSession(nodeSession)
		mh.OnEdgeNodeDisconnect(nodeInfo, connection)
	}()
}

func (mh *messageHandler) OnEdgeNodeConnect(info *model.HubInfo, connection conn.Connection) error {
	err := mh.MessageDispatcher.Publish(common.ConstructConnectMessage(info, true))
	if err != nil {
		common.NotifyEventQueueError(connection, info.NodeID)
		return err
	}

	return nil
}

func (mh *messageHandler) OnEdgeNodeDisconnect(info *model.HubInfo, connection conn.Connection) {
	err := mh.MessageDispatcher.Publish(common.ConstructConnectMessage(info, false))
	if err != nil {
		klog.Errorf("fail to publish node disconnect event for node %s, reason %s", info.NodeID, err.Error())
	}
}

func (mh *messageHandler) OnReadTransportErr(nodeID, projectID string) {
	klog.Errorf("projectID %s node %s read message err", projectID, nodeID)

	nodeSession, exist := mh.SessionManager.GetSession(nodeID)
	if !exist {
		klog.Errorf("session not found for node %s", nodeID)
		return
	}

	nodeSession.Terminating()
}
