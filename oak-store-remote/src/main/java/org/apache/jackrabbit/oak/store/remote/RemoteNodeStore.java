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
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.store.remote.store.ID;
import org.apache.jackrabbit.oak.store.remote.store.MemoryStorage;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.Store;
import org.apache.jackrabbit.oak.store.remote.store.Value;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class RemoteNodeStore implements NodeStore, Observable {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final ChangeDispatcher changeDispatcher;

    private final Store store;

    private final BlobStore blobStore;

    private final RemoteCheckpoints checkpoints;

    MemoryStorage storage = MemoryStorage.getInstance();

    public RemoteNodeStore(Store store, BlobStore blobStore) {
        this.store = store;
        this.blobStore = blobStore;
        this.changeDispatcher = new ChangeDispatcher(getRoot());
        this.checkpoints = new RemoteCheckpoints(store, blobStore);
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return changeDispatcher.addObserver(observer);
    }

    @Override
    public NodeState getRoot() {
//        ID rootID;
//
//        lock.readLock().lock();
//        try {
//            rootID = store.getTag("root");
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        } finally {
//            lock.readLock().unlock();
//        }
//
//        if (rootID == null) {
//            lock.writeLock().lock();
//            try {
//                rootID = store.getTag("root");
//                if (rootID == null) {
//                    rootID = store.putNode(
//                        store.putProperties(Collections.emptyMap()),
//                        store.putChildren(Collections.emptyMap())
//                    );
//                }
//                store.putTag("root", rootID);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            } finally {
//                lock.writeLock().unlock();
//            }
//        }

        MemoryStorage.Node root = storage.getNode("/");

        if (root == null) {
            root = storage.addNode("/", Collections.emptyMap());
        }

//        Node rootNode;
//        try {
//            rootNode = store.getNode(rootID);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

        return new RemoteNodeState("/", storage, blobStore);
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
            try {
                return merge(builder, builder.getBaseState(), builder.getNodeState(), commitHook, commitInfo);
            } catch (IOException e) {
                throw new CommitFailedException(CommitFailedException.OAK, -1, "I/O error", e);
            }
        }
        throw new IllegalArgumentException("builder");
    }

    private NodeState merge(RemoteNodeBuilder builder, NodeState baseState, NodeState headState, CommitHook commitHook, CommitInfo commitInfo) throws IOException, CommitFailedException {
        ID mergedID;

        lock.writeLock().lock();
        try {
            ID upstreamID = store.getTag("root");

            if (upstreamID == null) {
                throw new IllegalStateException("invalid upstream state");
            }

            ID baseID = null;

            if (baseState instanceof RemoteNodeState) {
                baseID = ((RemoteNodeState) baseState).getID();
            }

            if (baseID == null) {
                throw new IllegalStateException("invalid base state");
            }

            if (baseID.equals(upstreamID)) {
                mergedID = writeNode(commitHook.processCommit(baseState, headState, commitInfo));
            } else {
                NodeBuilder upstreamBuilder = new RemoteNodeState(store, blobStore, upstreamID, store.getNode(upstreamID)).builder();
                headState.compareAgainstBaseState(baseState, new ConflictAnnotatingRebaseDiff(upstreamBuilder));
                mergedID = writeNode(commitHook.processCommit(upstreamBuilder.getBaseState(), upstreamBuilder.getNodeState(), commitInfo));
            }

            store.putTag("root", mergedID);
        } finally {
            lock.writeLock().unlock();
        }

        NodeState mergedState = new RemoteNodeState(store, blobStore, mergedID, store.getNode(mergedID));
        changeDispatcher.contentChanged(mergedState, commitInfo);
        builder.reset(mergedState);
        return mergedState;
    }

    private ID writeNode(NodeState nodeState) throws IOException {

        // If the node state is already a KV node state, it's already persisted.
        // Just return its id.

        if (nodeState instanceof RemoteNodeState) {
            return ((RemoteNodeState) nodeState).getID();
        }

        // If the node state is a modified node state based on a KV node state,
        // check what changed and only persist those parts that changed.

        ModifiedNodeState after = null;

        if (nodeState instanceof ModifiedNodeState) {
            after = (ModifiedNodeState) nodeState;
        }

        RemoteNodeState before = null;

        if (after != null && after.getBaseState() instanceof RemoteNodeState) {
            before = (RemoteNodeState) after.getBaseState();
        }

        if (before != null) {
            return writeModifiedNode(before, after);
        }

        // The node state is neither a KV node state nor a modified node state,
        // so we have to create a new KV node out of it.

        Map<String, ID> children = new HashMap<>();

        for (ChildNodeEntry entry : nodeState.getChildNodeEntries()) {
            children.put(entry.getName(), writeNode(entry.getNodeState()));
        }

        Map<String, Value> properties = new HashMap<>();

        for (PropertyState propertyState : nodeState.getProperties()) {
            properties.put(propertyState.getName(), newValue(propertyState));
        }

        return store.putNode(
            store.putProperties(properties),
            store.putChildren(children)
        );
    }

    private ID writeModifiedNode(RemoteNodeState before, ModifiedNodeState after) throws IOException {
        class Flag {

            boolean value;

            Flag(boolean value) {
                this.value = value;
            }

        }

        Flag propertiesModified = new Flag(false);
        Flag childrenModified = new Flag(false);

        Map<String, ID> children = new HashMap<>(before.children());

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
            public boolean childNodeAdded(String name, NodeState after) {
                childrenModified.value = true;
                try {
                    children.put(name, writeNode(after));
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                return true;
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                childrenModified.value = true;
                try {
                    children.put(name, writeNode(after));
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                childrenModified.value = true;
                children.remove(name);
                return true;
            }

        });

        ID propertiesId = before.node().getProperties();

        if (propertiesModified.value) {
            Map<String, Value> properties = new HashMap<>();

            for (PropertyState propertyState : after.getProperties()) {
                properties.put(propertyState.getName(), newValue(propertyState));
            }

            propertiesId = store.putProperties(properties);
        }

        ID childrenId = before.node().getChildren();

        if (childrenModified.value) {
            childrenId = store.putChildren(children);
        }

        return store.putNode(propertiesId, childrenId);
    }

    private Value newValue(PropertyState ps) throws IOException {
        if (ps.getType() == Type.STRING) {
            return Value.newStringValue(ps.getValue(Type.STRING));
        }
        if (ps.getType() == Type.BINARY) {
            return Value.newBinaryValue(writeBlob(ps.getValue(Type.BINARY)));
        }
        if (ps.getType() == Type.LONG) {
            return Value.newLongValue(ps.getValue(Type.LONG));
        }
        if (ps.getType() == Type.DOUBLE) {
            return Value.newDoubleValue(ps.getValue(Type.DOUBLE));
        }
        if (ps.getType() == Type.DATE) {
            return Value.newDateValue(ps.getValue(Type.DATE));
        }
        if (ps.getType() == Type.BOOLEAN) {
            return Value.newBooleanValue(ps.getValue(Type.BOOLEAN));
        }
        if (ps.getType() == Type.NAME) {
            return Value.newNameValue(ps.getValue(Type.NAME));
        }
        if (ps.getType() == Type.PATH) {
            return Value.newPathValue(ps.getValue(Type.PATH));
        }
        if (ps.getType() == Type.REFERENCE) {
            return Value.newReferenceValue(ps.getValue(Type.REFERENCE));
        }
        if (ps.getType() == Type.WEAKREFERENCE) {
            return Value.newWeakReferenceValue(ps.getValue(Type.WEAKREFERENCE));
        }
        if (ps.getType() == Type.URI) {
            return Value.newURIValue(ps.getValue(Type.URI));
        }
        if (ps.getType() == Type.DECIMAL) {
            return Value.newDecimalValue(ps.getValue(Type.DECIMAL));
        }
        if (ps.getType() == Type.STRINGS) {
            return Value.newStringArray(ps.getValue(Type.STRINGS));
        }
        if (ps.getType() == Type.BINARIES) {
            return Value.newBinaryArray(writeBlobs(ps.getValue(Type.BINARIES)));
        }
        if (ps.getType() == Type.LONGS) {
            return Value.newLongArray(ps.getValue(Type.LONGS));
        }
        if (ps.getType() == Type.DOUBLES) {
            return Value.newDoubleArray(ps.getValue(Type.DOUBLES));
        }
        if (ps.getType() == Type.DATES) {
            return Value.newDateArray(ps.getValue(Type.DATES));
        }
        if (ps.getType() == Type.BOOLEANS) {
            return Value.newBooleanArray(ps.getValue(Type.BOOLEANS));
        }
        if (ps.getType() == Type.NAMES) {
            return Value.newNameArray(ps.getValue(Type.NAMES));
        }
        if (ps.getType() == Type.PATHS) {
            return Value.newPathArray(ps.getValue(Type.PATHS));
        }
        if (ps.getType() == Type.REFERENCES) {
            return Value.newReferenceArray(ps.getValue(Type.REFERENCES));
        }
        if (ps.getType() == Type.WEAKREFERENCES) {
            return Value.newWeakReferenceArray(ps.getValue(Type.WEAKREFERENCES));
        }
        if (ps.getType() == Type.URIS) {
            return Value.newURIArray(ps.getValue(Type.URIS));
        }
        if (ps.getType() == Type.DECIMALS) {
            return Value.newDecimalArray(ps.getValue(Type.DECIMALS));
        }
        throw new IllegalArgumentException("ps");
    }

    private Iterable<String> writeBlobs(Iterable<Blob> it) throws IOException {
        List<String> result = new ArrayList<>();

        for (Blob blob : it) {
            result.add(writeBlob(blob));
        }

        return result;
    }

    private String writeBlob(Blob blob) throws IOException {
        return blobStore.writeBlob(blob.getNewStream());
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
        ID upstreamID = store.getTag("root");

        if (upstreamID == null) {
            return headState;
        }

        ID baseID = null;

        if (baseState instanceof RemoteNodeState) {
            baseID = ((RemoteNodeState) baseState).getID();
        }

        if (baseID == null) {
            throw new IllegalStateException("invalid base state");
        }

        if (baseID.equals(upstreamID)) {
            return headState;
        }

        builder.reset(new RemoteNodeState(store, blobStore, upstreamID, store.getNode(upstreamID)));
        headState.compareAgainstBaseState(baseState, new ConflictAnnotatingRebaseDiff(builder));
        return builder.getNodeState();
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
        ID root;

        lock.readLock().lock();
        try {
            root = store.getTag("root");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }

        try {
            return checkpoints.checkpoint(root, lifetime, properties);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String checkpoint(long lifetime) {
        ID root;

        lock.readLock().lock();
        try {
            root = store.getTag("root");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }

        try {
            return checkpoints.checkpoint(root, lifetime);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
