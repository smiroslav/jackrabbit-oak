package org.apache.jackrabbit.oak.store.remote.store;

import org.apache.jackrabbit.oak.api.PropertyState;

import java.util.Collections;
import java.util.Map;

public class Node {
    private Map<String, PropertyState> properties;
    private String name;
    private long revision;
    private long revisionDeleted = Long.MAX_VALUE;

    public Node(String name, Map<String, PropertyState> properties, long revision) {
        this.properties = properties != null ? properties : Collections.emptyMap();
        this.name = name;
        this.revision = revision;
    }

    public long getRevision() {
        return revision;
    }

    public Map<String, PropertyState> getProperties() {
        return properties;
    }

    public String getName() {
        return name;
    }

    public boolean hasChildNode(String path) {
        return properties.containsKey(path);
    }

    public void setRevisionDeleted(long revisionDeleted) {
        this.revisionDeleted = revisionDeleted;
    }

    public boolean existsForRevision(long revision) {
        return  this.revision <= revision && revision < this.revisionDeleted;
    }
}
