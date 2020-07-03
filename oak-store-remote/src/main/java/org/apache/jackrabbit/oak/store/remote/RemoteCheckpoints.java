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

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.UUID.randomUUID;
import static org.apache.jackrabbit.oak.store.remote.store.Value.newLongValue;
import static org.apache.jackrabbit.oak.store.remote.store.Value.newStringValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jackrabbit.oak.store.remote.store.ID;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.Store;
import org.apache.jackrabbit.oak.store.remote.store.Value;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class RemoteCheckpoints {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Store store;

    private final BlobStore blobStore;

    RemoteCheckpoints(Store store, BlobStore blobStore) {
        this.store = store;
        this.blobStore = blobStore;
    }

    String checkpoint(ID root, long lifetime) throws IOException {
        return checkpoint(root, lifetime, emptyMap());
    }

    String checkpoint(ID root, long lifetime, Map<String, String> properties) throws IOException {
        String reference = randomUUID().toString();
        long now = currentTimeMillis();

        ID propertiesID = createProperties(properties);
        ID checkpointID = createCheckpoint(now, lifetime, propertiesID, root);
        createOrUpdateCheckpoints(now, reference, checkpointID);

        return reference;
    }

    private ID createProperties(Map<String, String> values) throws IOException {
        Map<String, Value> properties = new HashMap<>();

        for (Map.Entry<String, String> entry : values.entrySet()) {
            properties.put(entry.getKey(), newStringValue(entry.getValue()));
        }

        return store.putNode(
            store.putProperties(properties),
            store.putChildren(emptyMap())
        );
    }

    private ID createCheckpoint(long now, long lifetime, ID propertiesID, ID rootID) throws IOException {
        long timestamp;

        if (Long.MAX_VALUE - now > lifetime) {
            timestamp = now + lifetime;
        } else {
            timestamp = Long.MAX_VALUE;
        }

        Map<String, Value> properties = new HashMap<>();
        properties.put("timestamp", newLongValue(timestamp));
        properties.put("created", newLongValue(now));

        Map<String, ID> children = new HashMap<>();
        children.put("properties", propertiesID);
        children.put("root", rootID);

        return store.putNode(
            store.putProperties(properties),
            store.putChildren(children)
        );
    }

    private void createOrUpdateCheckpoints(long now, String reference, ID checkpoint) throws IOException {
        lock.writeLock().lock();
        try {
            ID id = store.getTag("checkpoints");

            if (id == null) {
                id = createCheckpoints(reference, checkpoint);
            } else {
                id = updateCheckpointsWithAdd(now, store.getNode(id), reference, checkpoint);
            }

            store.putTag("checkpoints", id);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private ID createCheckpoints(String reference, ID checkpoint) throws IOException {
        return store.putNode(
            store.putProperties(emptyMap()),
            store.putChildren(singletonMap(reference, checkpoint))
        );
    }

    private ID updateCheckpointsWithAdd(long now, Node node, String reference, ID checkpoint) throws IOException {
        Map<String, ID> children = new HashMap<>();

        Map<String, ID> previousChildren = store.getChildren(node.getChildren());

        if (previousChildren == null) {
            throw new IllegalStateException("children not found");
        }

        // Remove outdated checkpoints

        for (Entry<String, ID> e : previousChildren.entrySet()) {
            Node c = store.getNode(e.getValue());

            if (c == null) {
                continue;
            }

            Map<String, Value> p = store.getProperties(c.getProperties());

            if (p == null) {
                continue;
            }

            Value timestamp = p.get("timestamp");

            if (timestamp == null) {
                continue;
            }

            if (timestamp.asLongValue() < now) {
                continue;
            }

            children.put(e.getKey(), e.getValue());
        }

        children.put(reference, checkpoint);

        return store.putNode(node.getProperties(), store.putChildren(children));
    }

    private ID updateCheckpointsWithRemove(Node node, String reference) throws IOException {
        Map<String, ID> prev = store.getChildren(node.getChildren());

        if (prev == null) {
            throw new IllegalStateException("checkpoints children not found");
        }

        Map<String, ID> next = new HashMap<>(prev);
        next.remove(reference);
        return store.putNode(node.getProperties(), store.putChildren(next));
    }

    Map<String, String> checkpointInfo(String reference) throws IOException {
        ID checkpointsID;

        lock.readLock().lock();
        try {
            checkpointsID = store.getTag("checkpoints");
        } finally {
            lock.readLock().unlock();
        }

        if (checkpointsID == null) {
            return emptyMap();
        }

        Node checkpoints = store.getNode(checkpointsID);

        if (checkpoints == null) {
            throw new IllegalStateException("checkpoints node not found");
        }

        Map<String, ID> checkpointsChildren = store.getChildren(checkpoints.getChildren());

        if (checkpointsChildren == null) {
            throw new IllegalStateException("checkpoints children not found");
        }

        ID checkpointID = checkpointsChildren.get(reference);

        if (checkpointID == null) {
            return emptyMap();
        }

        Node checkpoint = store.getNode(checkpointID);

        if (checkpoint == null) {
            throw new IllegalStateException("checkpoint node not found");
        }

        Map<String, ID> checkpointChildren = store.getChildren(checkpoint.getChildren());

        if (checkpointChildren == null) {
            throw new IllegalStateException("checkpoint children not found");
        }

        ID propertiesID = checkpointChildren.get("properties");

        if (propertiesID == null) {
            throw new IllegalStateException("checkpoint properties ID not found");
        }

        Node properties = store.getNode(propertiesID);

        if (properties == null) {
            throw new IllegalStateException("checkpoint properties not found");
        }

        Map<String, Value> propertyValues = store.getProperties(properties.getProperties());

        if (propertyValues == null) {
            throw new IllegalStateException("checkpoint property values not found");
        }

        Map<String, String> values = new HashMap<>();

        for (Entry<String, Value> entry : propertyValues.entrySet()) {
            values.put(entry.getKey(), entry.getValue().asStringValue());
        }

        return values;
    }

    Iterable<String> checkpoints() throws IOException {
        ID checkpointsID;

        lock.readLock().lock();
        try {
            checkpointsID = store.getTag("checkpoints");
        } finally {
            lock.readLock().unlock();
        }

        if (checkpointsID == null) {
            return emptySet();
        }

        Node checkpoints = store.getNode(checkpointsID);

        if (checkpoints == null) {
            throw new IllegalStateException("checkpoints node not found");
        }

        Map<String, ID> checkpointsChildren = store.getChildren(checkpoints.getChildren());

        if (checkpointsChildren == null) {
            throw new IllegalStateException("checkpoints children not found");
        }

        Set<String> valid = new HashSet<>();

        long now = currentTimeMillis();

        for (Entry<String, ID> entry : checkpointsChildren.entrySet()) {
            Node checkpoint = store.getNode(entry.getValue());

            if (checkpoint == null) {
                throw new IllegalStateException("checkpoint nod not found");
            }

            Map<String, Value> checkpointProperties = store.getProperties(checkpoint.getProperties());

            if (checkpointProperties == null) {
                throw new IllegalStateException("checkpoint properties not found");
            }

            long timestamp = checkpointProperties.get("timestamp").asLongValue();

            if (timestamp < now) {
                continue;
            }

            valid.add(entry.getKey());
        }

        return valid;
    }

    NodeState retrieve(String reference) throws IOException {
        ID checkpointsID;

        lock.readLock().lock();
        try {
            checkpointsID = store.getTag("checkpoints");
        } finally {
            lock.readLock().unlock();
        }

        if (checkpointsID == null) {
            return null;
        }

        Node checkpoints = store.getNode(checkpointsID);

        if (checkpoints == null) {
            throw new IllegalStateException("checkpoints node not found");
        }

        Map<String, ID> checkpointsChildren = store.getChildren(checkpoints.getChildren());

        if (checkpointsChildren == null) {
            throw new IllegalStateException("checkpoints children not found");
        }

        ID checkpointID = checkpointsChildren.get(reference);

        if (checkpointID == null) {
            return null;
        }

        Node checkpoint = store.getNode(checkpointID);

        if (checkpoint == null) {
            throw new IllegalStateException("checkpoint node not found");
        }

        Map<String, ID> checkpointChildren = store.getChildren(checkpoint.getChildren());

        if (checkpointChildren == null) {
            throw new IllegalStateException("checkpoint children not found");
        }

        ID rootID = checkpointChildren.get("root");

        if (rootID == null) {
            throw new IllegalStateException("checkpoint root ID not found");
        }

        Node root = store.getNode(rootID);

        if (root == null) {
            throw new IllegalStateException("checkpoint root not found");
        }

        //return new RemoteNodeState(store, blobStore, rootID, root);
        return new RemoteNodeState(null, null, null, 1);
    }

    boolean release(String reference) throws IOException {
        lock.writeLock().lock();
        try {
            ID id = store.getTag("checkpoints");

            if (id == null) {
                return true;
            }

            Node checkpoints = store.getNode(id);

            if (checkpoints == null) {
                throw new IllegalStateException("checkpoints node not found");
            }

            Map<String, ID> checkpointsChildren = store.getChildren(checkpoints.getChildren());

            if (checkpointsChildren == null) {
                throw new IllegalStateException("checkpoints children not found");
            }

            if (checkpointsChildren.containsKey(reference)) {
                store.putTag("checkpoints", updateCheckpointsWithRemove(checkpoints, reference));
            }
        } finally {
            lock.writeLock().unlock();
        }
        return true;
    }

    Iterable<RemoteCheckpoint> getCheckpoints() throws IOException {
        ID id;

        lock.readLock().lock();
        try {
            id = store.getTag("checkpoints");
        } finally {
            lock.readLock().unlock();
        }

        if (id == null) {
            return emptyList();
        }

        Node checkpoints = store.getNode(id);

        if (checkpoints == null) {
            throw new IllegalStateException("checkpoints node not found");
        }

        Map<String, ID> checkpointsChildren = store.getChildren(checkpoints.getChildren());

        if (checkpointsChildren == null) {
            throw new IllegalStateException("checkpoints children not found");
        }

        List<RemoteCheckpoint> valid = new ArrayList<>();

        for (Entry<String, ID> e : checkpointsChildren.entrySet()) {
            addCheckpoint(valid, e.getKey(), e.getValue());
        }

        return valid;
    }

    private void addCheckpoint(List<RemoteCheckpoint> checkpoints, String handle, ID id) throws IOException {
        Node checkpoint = store.getNode(id);

        if (checkpoint == null) {
            throw new IllegalStateException("checkpoint node not found");
        }

        Map<String, Value> checkpointProperties = store.getProperties(checkpoint.getProperties());

        if (checkpointProperties == null) {
            throw new IllegalStateException("checkpoint properties not found");
        }

        long created = checkpointProperties.get("created").asLongValue();
        long timestamp = checkpointProperties.get("timestamp").asLongValue();

        Map<String, ID> checkpointChildren = store.getChildren(checkpoint.getChildren());

        if (checkpointChildren == null) {
            throw new IllegalStateException("checkpoint children not found");
        }

        ID propertiesID = checkpointChildren.get("properties");

        if (propertiesID == null) {
            throw new IllegalStateException("checkpoint properties ID not found");
        }

        Node properties = store.getNode(propertiesID);

        if (properties == null) {
            throw new IllegalStateException("checkpoint properties node not found");
        }

        Map<String, Value> propertyValues = store.getProperties(properties.getProperties());

        if (propertyValues == null) {
            throw new IllegalStateException("property values not found");
        }

        Map<String, String> normalized = new HashMap<>();

        for (Entry<String, Value> e : propertyValues.entrySet()) {
            normalized.put(e.getKey(), e.getValue().asStringValue());
        }

        checkpoints.add(new RemoteCheckpoint(handle, created, timestamp, normalized));
    }

}
