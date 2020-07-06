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

package org.apache.jackrabbit.oak.store.remote;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.TreeNode;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.store.remote.store.MemoryStorage;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.TreeMap;

import static java.util.stream.Collectors.toList;

class RemoteNodeState extends AbstractNodeState {

    private BlobStore blobStore = null;

    private String path;
    private MemoryStorage storage;

    Map<String, PropertyState> propertiesMap;
    MemoryStorage.Node remoteNode;

    private long revision;

    private Map<String, MemoryStorage.Node> childNodes;

    RemoteNodeState(String path, MemoryStorage storage, BlobStore blobStore, long revision) {
        this.path = path;
        this.storage = storage;
        this.blobStore = blobStore;
        this.revision = revision;
    }

    public String getNodePath() {
        return this.path;
    }

    @Override
    public TreeNode loadSubtree() {
        TreeMap<String, MemoryStorage.Node> subtree = storage.getNodeAndSubtree(getNodePath(), revision, true);

        Stack<TreeNode> stack = new Stack<>();
        for (String nodePath : subtree.descendingKeySet()) {
            MemoryStorage.Node node = subtree.get(nodePath);

            Map<String, TreeNode> children = getChildrenFromStack(nodePath, stack);
            TreeNode treeNode = new TreeNode(node.getName(), nodePath, children, node.getProperties());

            stack.push(treeNode);
        }
        return stack.isEmpty() ? null : stack.pop();
    }

    private Map<String, TreeNode> getChildrenFromStack(String parentNodePath, Stack<TreeNode> stack) {
        if(stack.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, TreeNode> result = new HashMap<>();

        while(!stack.isEmpty()) {
            TreeNode node = stack.peek();

            if(node.getPath().startsWith(parentNodePath)) {
                result.put(node.getName(), node);
                stack.remove(node);
            } else {
                break;
            }
        }

        return result;
    }

    public MemoryStorage.Node getNode() {
        if (remoteNode == null) {
            remoteNode = storage.getNode(this.path, revision);
        }
        return remoteNode;
    }

    @Override
    public boolean exists() {
        return null != getNode();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        if (getNode() == null) {
            return Collections.emptyList();
        } else {
            return getNode().getProperties().values();
        }
    }

    @Override
    public boolean hasChildNode(String name) {
        return null != getChildNodesMap().get(getChildNodePath(name));
    }

    private String getChildNodePath(String name) {
        if (this.path.equals("/")) {
            return this.path + name;
        } else {
            return this.path + "/" + name;
        }
    }

    @Override
    public NodeState getChildNode(String name) throws IllegalArgumentException {
        String childPath = getNodePath().equals("/") ? getNodePath() + name : getNodePath() + "/" + name;

        return new RemoteNodeState(childPath, storage, blobStore, revision);
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return getChildNodesMap().values().stream().map(this::createChildNodeEntry).collect(toList());
    }

    private Map<String, MemoryStorage.Node> getChildNodesMap() {
        if (this.childNodes == null) {
            this.childNodes = storage.getNodeAndSubtree(getNodePath(), revision, false);
            this.childNodes.remove(this.path);
        }
        return this.childNodes;
    }

    private ChildNodeEntry createChildNodeEntry(MemoryStorage.Node node) {
        return new ChildNodeEntry() {

            @Override
            public String getName() {
                return node.getName();
            }

            @Override
            public NodeState getNodeState() {
                return getChildNode(node.getName());
            }

        };
    }

    @Override
    public NodeBuilder builder() {
        return new RemoteNodeBuilder(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (o instanceof RemoteNodeState) {
            if (getNodePath().equals(((RemoteNodeState) o).getNodePath()) && revision == ((RemoteNodeState) o).revision) {
                return true;
            }
        }
        if (o instanceof NodeState) {
            return AbstractNodeState.equals(this, (NodeState) o);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodePath(), revision);
    }

    private ChildNodeEntry newChildNodeEntry(String name) {
        return new ChildNodeEntry() {

            @Override
            public String getName() {
                return name;
            }

            @Override
            public NodeState getNodeState() {
                return getChildNode(name);
            }

        };
    }

    @Override
    public String toString() {
        return String.format("RemoteNodeState{path=%s}", getNodePath());
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base == this) {
            return true;
        }
        if (base instanceof RemoteNodeState) {
            return compareAgainstBaseState((RemoteNodeState) base, diff);
        }
        if (base == EmptyNodeState.EMPTY_NODE) {
            return EmptyNodeState.compareAgainstEmptyState(this, diff);
        }
        if (base.exists()) {
            return AbstractNodeState.compareAgainstBaseState(this, base, diff);
        }
        return EmptyNodeState.compareAgainstEmptyState(this, diff);
    }

    private boolean compareAgainstBaseState(RemoteNodeState base, NodeStateDiff diff) {
        if (base.equals(this)) {
            return true;
        }

        if (!AbstractNodeState.compareAgainstBaseState(this, base, diff)) {
            return false;
        }
        return true;
    }
}
