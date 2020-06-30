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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

    private final BlobStore blobStore;

    private ID id;

    private Node node;

    private String path;
    private MemoryStorage storage;

    private Map<String, Value> properties;

    private Map<String, ID> children;

    Map<String, PropertyState> propertiesMap;
    MemoryStorage.Node remoteNode;

    RemoteNodeState(Store store, BlobStore blobStore, ID id, Node node) {
        this.store = store;
        this.blobStore = blobStore;
        this.id = id;
        this.node = node;
    }

    RemoteNodeState(String path, MemoryStorage storage, BlobStore blobStore) {
        this.path = path;
        this.storage = storage;
        this.blobStore = blobStore;
    }

    public String getNodePath() {
        return this.path;
    }

    public MemoryStorage.Node getNode() {
        if (remoteNode == null) {
            remoteNode = storage.getNode(this.path);
        }
        return remoteNode;
    }

    public ID getID() {
        return id;
    }

    Node node() {
        return node;
    }

    Map<String, Value> properties() {
        if (properties == null) {
            try {
                properties = store.getProperties(node.getProperties());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return properties;
    }

    Map<String, ID> children() {
        if (children == null) {
            try {
                children = store.getChildren(node.getChildren());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return children;
    }

    @Override
    public boolean exists() {
        //return true;
        return null != getNode();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        //return properties().entrySet().stream().map(this::newPropertyState).collect(toList());
        return getNode().getProperties().values();
    }

    @Override
    public boolean hasChildNode(String name) {
        //return children().containsKey(name);
        return getNode().hasChildNode(name);
    }

    @Override
    public NodeState getChildNode(String name) throws IllegalArgumentException {
        /*ID childId = children().get(name);

        if (childId == null) {
            return EmptyNodeState.MISSING_NODE;
        }

        Node child;

        try {
            child = store.getNode(childId);
        } catch (IOException e) {
            return EmptyNodeState.MISSING_NODE;
        }

        return new RemoteNodeState(store, blobStore, childId, child);
        */
        String childPath = getNodePath().equals("/") ? getNodePath() + name : getNodePath() + "/" + name;

        return new RemoteNodeState(childPath, storage, blobStore);
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        SortedMap<String, MemoryStorage.Node> subtree = storage.getNodeAndSubtree(getNodePath());

        return subtree.values().stream().map(this::createChildNodeEntry).collect(toList());

        //return children().keySet().stream().map(this::newChildNodeEntry).collect(toList());
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
//            if (id.equals(((RemoteNodeState) o).id)) {
//                return true;
//            }
            if (getNodePath().equals(((RemoteNodeState) o).getNodePath())) {
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
        //return id.hashCode();
        return getNodePath().hashCode();
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

    private PropertyState newPropertyState(Map.Entry<String, Value> e) {
        return newPropertyState(e.getKey(), e.getValue());
    }

    private PropertyState newPropertyState(String name, Value value) {
        return PropertyStates.createProperty(name, convert(value), newType(value));
    }

    private Object convert(Value value) {
        if (value.isArray()) {
            return convertArray(value);
        }
        return convertValue(value);
    }

    private Object convertValue(Value value) {
        switch (value.getType()) {
            case STRING:
                return value.asStringValue();
            case BINARY:
                return convertBlobID(value.asStringValue());
            case LONG:
                return value.asLongValue();
            case DOUBLE:
                return value.asDoubleValue();
            case DATE:
                return value.asStringValue();
            case BOOLEAN:
                return value.asBooleanValue();
            case NAME:
                return value.asStringValue();
            case PATH:
                return value.asStringValue();
            case REFERENCE:
                return value.asStringValue();
            case WEAK_REFERENCE:
                return value.asStringValue();
            case URI:
                return value.asStringValue();
            case DECIMAL:
                return value.asDecimalValue();
            default:
                throw new IllegalArgumentException("value");
        }
    }

    private Blob convertBlobID(String blobID) {
        return new RemoteBlob(blobStore, blobID);
    }

    private Object convertArray(Value value) {
        switch (value.getType()) {
            case STRING:
                return value.asStringArray();
            case BINARY:
                return convertBlobIDs(value.asStringArray());
            case LONG:
                return value.asLongArray();
            case DOUBLE:
                return value.asDoubleArray();
            case DATE:
                return value.asStringArray();
            case BOOLEAN:
                return value.asBooleanArray();
            case NAME:
                return value.asStringArray();
            case PATH:
                return value.asStringArray();
            case REFERENCE:
                return value.asStringArray();
            case WEAK_REFERENCE:
                return value.asStringArray();
            case URI:
                return value.asStringArray();
            case DECIMAL:
                return value.asDecimalArray();
            default:
                throw new IllegalArgumentException("value");
        }
    }

    private Iterable<Blob> convertBlobIDs(Iterable<String> references) {
        List<Blob> blobs = new ArrayList<>();
        for (String reference : references) {
            blobs.add(convertBlobID(reference));
        }
        return blobs;
    }

    private static Type newType(Value value) {
        if (value.isArray()) {
            return newArrayType(value.getType());
        }
        return newValueType(value.getType());
    }

    private static Type newValueType(org.apache.jackrabbit.oak.store.remote.store.Type type) {
        switch (type) {
            case STRING:
                return Type.STRING;
            case BINARY:
                return Type.BINARY;
            case LONG:
                return Type.LONG;
            case DOUBLE:
                return Type.DOUBLE;
            case DATE:
                return Type.DATE;
            case BOOLEAN:
                return Type.BOOLEAN;
            case NAME:
                return Type.NAME;
            case PATH:
                return Type.PATH;
            case REFERENCE:
                return Type.REFERENCE;
            case WEAK_REFERENCE:
                return Type.WEAKREFERENCE;
            case URI:
                return Type.URI;
            case DECIMAL:
                return Type.DECIMAL;
            default:
                throw new IllegalArgumentException("type");
        }
    }

    private static Type newArrayType(org.apache.jackrabbit.oak.store.remote.store.Type type) {
        switch (type) {
            case STRING:
                return Type.STRINGS;
            case BINARY:
                return Type.BINARIES;
            case LONG:
                return Type.LONGS;
            case DOUBLE:
                return Type.DOUBLES;
            case DATE:
                return Type.DATES;
            case BOOLEAN:
                return Type.BOOLEANS;
            case NAME:
                return Type.NAMES;
            case PATH:
                return Type.PATHS;
            case REFERENCE:
                return Type.REFERENCES;
            case WEAK_REFERENCE:
                return Type.WEAKREFERENCES;
            case URI:
                return Type.URIS;
            case DECIMAL:
                return Type.DECIMALS;
            default:
                throw new IllegalArgumentException("type");
        }
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
        if (base.id.equals(id)) {
            return true;
        }
        //if (!base.node.getProperties().equals(node.getProperties())) {
            if (!AbstractNodeState.comparePropertiesAgainstBaseState(this, base, diff)) {
                return false;
            }
        //}
        if (!base.node.getChildren().equals(node.getChildren())) {
            return compareChildrenAgainstBaseState(base, diff);
        }
        return true;
    }

    private boolean compareChildrenAgainstBaseState(RemoteNodeState base, NodeStateDiff diff) {
        for (Entry<String, ID> entry : children().entrySet()) {
            if (base.children().containsKey(entry.getKey())) {
                if (!base.children().get(entry.getKey()).equals(entry.getValue())) {
                    if (!diff.childNodeChanged(entry.getKey(), base.getChildNode(entry.getKey()), getChildNode(entry.getKey()))) {
                        return false;
                    }
                }
            } else {
                if (!diff.childNodeAdded(entry.getKey(), getChildNode(entry.getKey()))) {
                    return false;
                }
            }
        }

        for (ChildNodeEntry childNode : getChildNodeEntries()) {

        }
        for (Entry<String, ID> entry : base.children().entrySet()) {
            if (!children().containsKey(entry.getKey())) {
                if (!diff.childNodeDeleted(entry.getKey(), base.getChildNode(entry.getKey()))) {
                    return false;
                }
            }
        }
        return true;
    }

}
