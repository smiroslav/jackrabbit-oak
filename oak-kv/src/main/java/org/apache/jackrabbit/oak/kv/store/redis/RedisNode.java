package org.apache.jackrabbit.oak.kv.store.redis;

import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;

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
