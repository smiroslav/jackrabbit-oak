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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.store.remote.store.MemoryStorage;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.store.remote.store.Node;

public class RemoteNodeStore implements NodeStore, Observable {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final ChangeDispatcher changeDispatcher;

    private final BlobStore blobStore;

    private RemoteCheckpoints checkpoints;

    MemoryStorage storage;

    public RemoteNodeStore(MemoryStorage storage, BlobStore blobStore) {
        this.storage = storage;
        this.blobStore = blobStore;
        this.changeDispatcher = new ChangeDispatcher(getRoot());
        //TODO this.checkpoints = new RemoteCheckpoints(store, blobStore);
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return changeDispatcher.addObserver(observer);
    }

    @Override
    public NodeState getRoot() {

        Node root = storage.getRootNode();

        if (root == null) {
            storage.addNode("/", Collections.emptyList());
        }

        root = storage.getRootNode();

        return new RemoteNodeState("/", storage, blobStore, root.getRevision());
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        if (builder instanceof RemoteNodeBuilder) {
            return merge((RemoteNodeBuilder) builder, commitHook, info);
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState merge(RemoteNodeBuilder builder, CommitHook commitHook, CommitInfo commitInfo) throws CommitFailedException {
        if (builder.isRootBuilder()) {
            storage.incrementRevisionNumber();
            try {
                return merge(builder, builder.getBaseState(), builder.getNodeState(), commitHook, commitInfo);
            } catch (IOException e) {
                throw new CommitFailedException(CommitFailedException.OAK, -1, "I/O error", e);
            }
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState merge(RemoteNodeBuilder builder, NodeState baseState, NodeState headState, CommitHook commitHook, CommitInfo commitInfo) throws IOException, CommitFailedException {
        lock.writeLock().lock();
        try {
            if (baseState.getNodePath().equals("/")) {
                writeNode(commitHook.processCommit(baseState, headState, commitInfo));
            } else {
                NodeBuilder upstreamBuilder = new RemoteNodeState(baseState.getNodePath(), storage, blobStore, storage.getCurrentRevision()).builder();
                headState.compareAgainstBaseState(baseState, new ConflictAnnotatingRebaseDiff(upstreamBuilder));
                writeNode(commitHook.processCommit(upstreamBuilder.getBaseState(), upstreamBuilder.getNodeState(), commitInfo));
            }
        } finally {
            lock.writeLock().unlock();
        }

        NodeState mergedState = new RemoteNodeState(baseState.getNodePath(), storage, blobStore, storage.getCurrentRevision());
        changeDispatcher.contentChanged(mergedState, commitInfo);
        builder.reset(mergedState);

        return mergedState;
    }

    private void writeNode(NodeState nodeState) throws IOException {

        //if node exist return
        if (nodeState instanceof RemoteNodeState) {
            if (nodeState.exists()) {
                return;
            }
        }

        ModifiedNodeState after = null;

        if (nodeState instanceof ModifiedNodeState) {
            after = (ModifiedNodeState) nodeState;
        }

        RemoteNodeState before = null;

        if (after != null && after.getBaseState() instanceof RemoteNodeState) {
            before = (RemoteNodeState) after.getBaseState();
        }

        if (before != null) {
            writeModifiedNode(before, after);
            return;
        }

        // The node state is neither a RemoteNodeState node state nor a modified node state,
        // so we have to create a new RemoteNodeState node out of it.

        Map<String, PropertyState> properties = new HashMap<>();

        for (PropertyState propertyState : nodeState.getProperties()) {
            properties.put(propertyState.getName(), propertyState);
        }

        NodeState missing = EmptyNodeState.MISSING_NODE;
        if (after != null) {
            writeModifiedNode(missing , after);
        } else {
            writeModifiedNode(missing , nodeState);
        }
    }

    private void writeModifiedNode(NodeState before, NodeState after) throws IOException {
        class Flag {

            boolean value;

            Flag(boolean value) {
                this.value = value;
            }

        }

        Flag propertiesModified = new Flag(false);
        Flag childrenModified = new Flag(false);

        if (before.exists() && after.getNodePath() == null) {
            after.setNodePath(before.getNodePath());
        }

        after.compareAgainstBaseState(before, new NodeStateDiff() {

            @Override
            public boolean propertyAdded(PropertyState after) {
                propertiesModified.value = true;
                return true;
            }

            @Override
            public boolean propertyChanged(PropertyState before, PropertyState after) {
                propertiesModified.value = true;
                return true;
            }

            @Override
            public boolean propertyDeleted(PropertyState before) {
                propertiesModified.value = true;
                return true;
            }

            @Override
            public boolean childNodeAdded(String name, NodeState child) {
                childrenModified.value = true;
                try {
                    child.setNodePath(getChildNodePath(after.getNodePath(), name));
                    writeNode(child);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                return true;
            }

            private String getChildNodePath(String nodePath, String name) {
                if (nodePath.equals("/")) {
                    return nodePath + name;
                } else {
                    return nodePath + "/" + name;
                }
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState child) {
                childrenModified.value = true;
                try {
                    child.setNodePath(getChildNodePath(after.getNodePath(), name));
                    writeNode(child);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState child) {
                childrenModified.value = true;
                storage.deleteNode(getChildNodePath(after.getNodePath(), name));
                return true;
            }

        });
        storage.addNode(after.getNodePath(), after.getProperties());

        if(before.exists() && !before.getNodePath().equals(after.getNodePath())) {
            storage.moveChildNodes(before.getNodePath(), after.getNodePath());
        }
    }

    @Override
    public NodeState rebase(NodeBuilder builder) {
        if (builder instanceof RemoteNodeBuilder) {
            return rebase((RemoteNodeBuilder) builder);
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState rebase(RemoteNodeBuilder builder) {
        if (builder.isRootBuilder()) {
            try {
                return rebase(builder, builder.getBaseState(), builder.getNodeState());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState rebase(RemoteNodeBuilder builder, NodeState baseState, NodeState headState) throws IOException {

        return null;
    }

    @Override
    public NodeState reset(NodeBuilder builder) {
        if (builder instanceof RemoteNodeBuilder) {
            return reset((RemoteNodeBuilder) builder);
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState reset(RemoteNodeBuilder builder) {
        if (builder.isRootBuilder()) {
            NodeState root = getRoot();
            builder.reset(root);
            return root;
        }
        throw new IllegalArgumentException("builder");
    }

    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        return new RemoteBlob(blobStore, blobStore.writeBlob(inputStream));
    }

    @Override
    public Blob getBlob(String reference) {
        String blobId = blobStore.getBlobId(reference);

        if (blobId == null) {
            return null;
        }

        return new RemoteBlob(blobStore, blobId);
    }

    @Override
    public String checkpoint(long lifetime, Map<String, String> properties) {

        return null;
    }

    @Override
    public String checkpoint(long lifetime) {

        return null;
    }

    @Override
    public Map<String, String> checkpointInfo(String checkpoint) {
        try {
            return checkpoints.checkpointInfo(checkpoint);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<String> checkpoints() {
        try {
            return checkpoints.checkpoints();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public NodeState retrieve(String checkpoint) {
        try {
            return checkpoints.retrieve(checkpoint);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean release(String checkpoint) {
        try {
            return checkpoints.release(checkpoint);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterable<RemoteCheckpoint> getCheckpoints() throws IOException {
        return checkpoints.getCheckpoints();
    }

}
