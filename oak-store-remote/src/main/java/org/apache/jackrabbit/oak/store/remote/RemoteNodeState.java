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

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.store.remote.store.ID;
import org.apache.jackrabbit.oak.store.remote.store.MemoryStorage;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.Store;
import org.apache.jackrabbit.oak.store.remote.store.Value;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

class RemoteNodeState extends AbstractNodeState {

    private Store store;

    private BlobStore blobStore = null;

    private ID id;

    private Node node;

    private String path;
    private MemoryStorage storage;

    private Map<String, Value> properties;

    private Map<String, ID> children;

    Map<String, PropertyState> propertiesMap;
    MemoryStorage.Node remoteNode;

    private long revision;

    private Map<String, MemoryStorage.Node> childNodes;

    RemoteNodeState(Store store, BlobStore blobStore, ID id, Node node) {
        this.store = store;
        this.blobStore = blobStore;
        this.id = id;
        this.node = node;
    }

    RemoteNodeState(String path, MemoryStorage storage, BlobStore blobStore, long revision) {
        this.path = path;
        this.storage = storage;
        this.blobStore = blobStore;
        this.revision = revision;
    }

    public String getNodePath() {
        return this.path;
    }

    public MemoryStorage.Node getNode() {
        if (remoteNode == null) {
            remoteNode = storage.getNode(this.path, revision);
        }
        return remoteNode;
    }

    public ID getID() {
        return id;
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
        //return children().containsKey(name);
        //return getNode().hasChildNode(name);
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
