package org.apache.jackrabbit.oak.store.remote.store.redis;

import org.apache.jackrabbit.oak.store.remote.store.ID;
import org.apache.jackrabbit.oak.store.remote.store.Node;

class RedisNode implements Node {

    private final ID properties;

    private final ID children;

    RedisNode(ID properties, ID children) {
        this.properties = properties;
        this.children = children;
    }

    @Override
    public ID getProperties() {
        return properties;
    }

    @Override
    public ID getChildren() {
        return children;
    }

}
