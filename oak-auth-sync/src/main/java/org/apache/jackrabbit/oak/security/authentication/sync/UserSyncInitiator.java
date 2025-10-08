/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.security.authentication.sync;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observer that tracks changes under the /home path and converts them to JSON format
 * using JsopDiff.
 */
public class UserSyncInitiator implements Observer {
    private static final Logger LOG = LoggerFactory.getLogger(UserSyncInitiator.class);
    private static final String DEFAULT_HOME_PATH = "/home";
    private NodeState previousRoot = EmptyNodeState.EMPTY_NODE;

    private final String instanceId;
    private final String homePath;

    private final UserDiffPayloadQueue payloadQueue;

    private Oak oak;

    NodeState uuidIndex;

    public UserSyncInitiator(String instanceId, UserDiffPayloadQueue payloadQueue) {
        this(instanceId, payloadQueue, DEFAULT_HOME_PATH);
    }

    public UserSyncInitiator(String instanceId, UserDiffPayloadQueue payloadQueue, String homePath) {
        this.instanceId = instanceId;
        this.payloadQueue = payloadQueue;
        this.homePath = homePath;

        this.uuidIndex = EmptyNodeState.EMPTY_NODE;
    }

    @Override
    public void contentChanged(@NotNull NodeState rootNodeState, @NotNull CommitInfo info) {

        // Get the home node states
        NodeState oldHome = NodeStateUtils.getNode(previousRoot, this.homePath);
        NodeState newHome = NodeStateUtils.getNode(rootNodeState, this.homePath);

        // Use JsopDiff to generate the JSON diff
        JsopDiff diff = new JsopDiff(this.homePath, Integer.MAX_VALUE, uuid -> getUserId(previousRoot, rootNodeState, uuid));
        newHome.compareAgainstBaseState(oldHome, diff);

        // If we have changes, process them
        String changes = diff.toString();
        if (!changes.isEmpty()) {
            enqueueChange(changes);
        }

        previousRoot = rootNodeState;
    }



    private String getUserId(NodeState previousRoot, NodeState newRoot,  String puuid){

        String uuid = puuid.substring(1, puuid.length());

        NodeState root = puuid.startsWith("+") ? newRoot : previousRoot;

        PropertyState entry = root.getChildNode("oak:index").getChildNode("uuid").getChildNode(":index").getChildNode(uuid).getProperty("entry");

        NodeState nodeState = root;

        if (entry != null && entry.isArray() && entry.count() > 0) {
            String nodePath = entry.getValue(Type.STRING, 0);

            for (String pathSegment : PathUtils.elements(nodePath)) {
                nodeState = nodeState.getChildNode(pathSegment);
            }
        }

        return nodeState.getString("rep:authorizableId");
    }

    /**
     * Process a change detected under /home. Override this method to handle changes.
     * @param change the JSON representation of the change
     */
    protected void enqueueChange(String change) {
        LOG.info("Changes under /home: {}", change);
        payloadQueue.enqueue(change, instanceId);
    }
}