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

package org.apache.jackrabbit.oak.store.remote.store.memory;

import static java.util.Collections.emptyMap;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jackrabbit.oak.store.remote.store.ID;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.Store;
import org.apache.jackrabbit.oak.store.remote.store.Value;

public class MemoryStore implements Store {

    private static final ID emptyPropertiesId = new ID() {

        // Intentionally left blank.

    };

    private static final ID emptyChildrenId = new ID() {

        // Intentionally left blank.

    };

    private static final ID emptyNodeId = new ID() {

        // Intentionally left blank.

    };

    private static final Node emptyNode = new Node() {

        @Override
        public ID getProperties() {
            return emptyPropertiesId;
        }

        @Override
        public ID getChildren() {
            return emptyChildrenId;
        }

    };

    private static final Map<String, Value> emptyProperties = emptyMap();

    private static final Map<String, ID> emptyChildren = emptyMap();

    private final AtomicLong sequence = new AtomicLong();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<String, ID> tags = new HashMap<>();

    private final Map<ID, Node> nodes = new WeakHashMap<>();

    private final Map<ID, Map<String, Value>> properties = new WeakHashMap<>();

    private final Map<ID, Map<String, ID>> children = new WeakHashMap<>();

    @Override
    public ID getTag(String tag) {
        lock.readLock().lock();
        try {
            return tags.get(tag);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void putTag(String tag, ID id) {
        lock.writeLock().lock();
        try {
            tags.put(tag, id);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void deleteTag(String tag) {
        lock.writeLock().lock();
        try {
            tags.remove(tag);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Node getNode(ID id) {
        if (id == emptyNodeId) {
            return emptyNode;
        }

        lock.readLock().lock();
        try {
            return nodes.get(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ID putNode(ID properties, ID children) {
        if (properties == emptyPropertiesId && children == emptyChildrenId) {
            return emptyNodeId;
        }

        ID id = new MemoryID(sequence.getAndIncrement());

        lock.writeLock().lock();
        try {
            nodes.put(id, new MemoryNode(properties, children));
        } finally {
            lock.writeLock().unlock();
        }

        return id;
    }

    @Override
    public Map<String, Value> getProperties(ID id) {
        if (id == emptyPropertiesId) {
            return emptyProperties;
        }

        Map<String, Value> properties;

        lock.readLock().lock();
        try {
            properties = this.properties.get(id);
        } finally {
            lock.readLock().unlock();
        }

        if (properties == null) {
            return null;
        }

        return new HashMap<>(properties);
    }

    @Override
    public ID putProperties(Map<String, Value> properties) {
        if (properties.isEmpty()) {
            return emptyPropertiesId;
        }

        ID id = new MemoryID(sequence.getAndIncrement());
        Map<String, Value> copy = new HashMap<>(properties);

        lock.writeLock().lock();
        try {
            this.properties.put(id, copy);
        } finally {
            lock.writeLock().unlock();
        }

        return id;
    }

    @Override
    public Map<String, ID> getChildren(ID id) {
        if (id == emptyChildrenId) {
            return emptyChildren;
        }

        Map<String, ID> children;

        lock.readLock().lock();
        try {
            children = this.children.get(id);
        } finally {
            lock.readLock().unlock();
        }

        if (children == null) {
            return null;
        }

        return new HashMap<>(children);
    }

    @Override
    public ID putChildren(Map<String, ID> children) {
        if (children.isEmpty()) {
            return emptyChildrenId;
        }

        ID id = new MemoryID(sequence.getAndIncrement());
        Map<String, ID> copy = new HashMap<>(children);

        lock.writeLock().lock();
        try {
            this.children.put(id, copy);
        } finally {
            lock.writeLock().unlock();
        }

        return id;
    }

}
