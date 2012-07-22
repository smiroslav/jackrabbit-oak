/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Basic in-memory node state builder.
 */
public class MemoryNodeStateBuilder implements NodeStateBuilder {

    private final NodeState base;

    /**
     * Set of added, modified or removed ({@code null} value) property states.
     */
    private Map<String, PropertyState> properties = Maps.newHashMap();

    /**
     * Set of builders for added, modified or removed ({@code null} value)
     * child nodes.
     */
    private final Map<String, NodeStateBuilder> builders = Maps.newHashMap();

    /**
     * Flag to indicate that the current {@link #properties} map is being
     * referenced by a {@link ModifiedNodeState} instance returned by a
     * previous {@link #getNodeState()} call, and thus should not be
     * modified unless first explicitly {@link #unfreeze() unfrozen}.
     */
    private boolean frozen = false;

    /**
     * Creates a new in-memory node state builder.
     *
     * @param base base state of the new builder, or {@code null}
     */
    public MemoryNodeStateBuilder(NodeState base) {
        if (base != null) {
            this.base = base;
        } else {
            this.base = MemoryNodeState.EMPTY_NODE;
        }
    }

    /**
     * Factory method for creating new child state builders. Subclasses may
     * override this method to control the behavior of child state builders.
     *
     * @param child base state of the new builder, or {@code null}
     * @return new builder
     */
    protected MemoryNodeStateBuilder createChildBuilder(NodeState child) {
        return new MemoryNodeStateBuilder(child);
    }

    /**
     * Called whenever <em>this</em> node is modified, i.e. a property is
     * added, changed or removed, or a child node is added or removed. Changes
     * inside child nodes or the subtrees below are not reported. The default
     * implementation does nothing, but subclasses may override this method
     * to better track changes.
     */
    protected void updated() {
        // do nothing
    }

    /**
     * Ensures that the current {@link #properties} map is not {@link #frozen}.
     */
    private void unfreeze() {
        if (frozen) {
            properties = new HashMap<String, PropertyState>(properties);
            frozen = false;
        }
    }

    @Override
    public NodeState getNodeState() {
        Map<String, PropertyState> props = Collections.emptyMap();
        if (!properties.isEmpty()) {
            frozen = true;
            props = properties;
        }

        Map<String, NodeState> nodes = Collections.emptyMap();
        if (!builders.isEmpty()) {
            nodes = new HashMap<String, NodeState>(builders.size() * 2);
            for (Map.Entry<String, NodeStateBuilder> entry
                    : builders.entrySet()) {
                NodeStateBuilder builder = entry.getValue();
                if (builder != null) {
                    nodes.put(entry.getKey(), builder.getNodeState());
                } else {
                    nodes.put(entry.getKey(), null);
                }
            }
        }

        if (props.isEmpty() && nodes.isEmpty()) {
            return base;
        } else {
            return new ModifiedNodeState(base, props, nodes);
        }
    }

    @Override
    public long getChildNodeCount() {
        long count = base.getChildNodeCount();
        for (Map.Entry<String, NodeStateBuilder> entry : builders.entrySet()) {
            NodeState before = base.getChildNode(entry.getKey());
            NodeStateBuilder after = entry.getValue();
            if (before == null && after != null) {
                count++;
            } else if (before != null && after == null) {
                count--;
            }
        }
        return count;
    }

    public boolean hasChildNode(String name) {
        NodeStateBuilder builder = builders.get(name);
        if (builder != null) {
            return true;
        } else if (builders.containsKey(name)) {
            return false;
        } else {
            return base.getChildNode(name) != null;
        }
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        Iterable<String> unmodified = Iterables.transform(
                base.getChildNodeEntries(),
                new Function<ChildNodeEntry, String>() {
                    @Override
                    public String apply(ChildNodeEntry input) {
                        return input.getName();
                    }
                });
        Predicate<String> unmodifiedFilter = Predicates.not(Predicates.in(
                ImmutableSet.copyOf(builders.keySet())));
        Set<String> modified = ImmutableSet.copyOf(
                Maps.filterValues(builders, Predicates.notNull()).keySet());
        return Iterables.concat(
                Iterables.filter(unmodified, unmodifiedFilter),
                modified);
    }

    @Override
    public void setNode(String name, NodeState nodeState) {
        if (nodeState == null) {
            removeNode(name);
        } else {
            if (nodeState.equals(base.getChildNode(name))) {
                builders.remove(name);
            } else {
                builders.put(name, createChildBuilder(nodeState));
            }
            updated();
        }
    }

    @Override
    public void removeNode(String name) {
        if (base.getChildNode(name) != null) {
            builders.put(name, null);
        } else {
            builders.remove(name);
        }
        updated();
    }

    @Override
    public long getPropertyCount() {
        long count = base.getPropertyCount();
        for (Map.Entry<String, PropertyState> entry : properties.entrySet()) {
            PropertyState before = base.getProperty(entry.getKey());
            PropertyState after = entry.getValue();
            if (before == null && after != null) {
                count++;
            } else if (before != null && after == null) {
                count--;
            }
        }
        return count;
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        frozen = true;
        final Set<String> names = properties.keySet();
        Predicate<PropertyState> predicate = new Predicate<PropertyState>() {
            @Override
            public boolean apply(PropertyState input) {
                return !names.contains(input.getName());
            }
        };
        return Iterables.concat(
                Iterables.filter(properties.values(), Predicates.notNull()),
                Iterables.filter(base.getProperties(), predicate));
    }


    @Override
    public PropertyState getProperty(String name) {
        PropertyState property = properties.get(name);
        if (property != null || properties.containsKey(name)) {
            return property;
        } else {
            return base.getProperty(name);
        }
    }

    @Override
    public void setProperty(String name, CoreValue value) {
        unfreeze();
        properties.put(name, new SinglePropertyState(name, value));
        updated();
    }

    @Override
    public void setProperty(String name, List<CoreValue> values) {
        unfreeze();
        if (values.isEmpty()) {
            properties.put(name, new EmptyPropertyState(name));
        } else {
            properties.put(name, new MultiPropertyState(name, values));
        }
        updated();
    }

    @Override
    public void removeProperty(String name) {
        unfreeze();
        if (base.getProperty(name) != null) {
            properties.put(name, null);
        } else {
            properties.remove(name);
        }
        updated();
    }

    @Override
    public NodeStateBuilder getChildBuilder(String name) {
        NodeStateBuilder builder = builders.get(name);
        if (builder == null) {
            NodeState baseState = base.getChildNode(name);
            builder = createChildBuilder(baseState);
            builders.put(name, builder);
        }
        return builder;
    }

}
