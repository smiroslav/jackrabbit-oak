package org.apache.jackrabbit.oak.spi.state;

import org.apache.jackrabbit.oak.api.PropertyState;

import java.util.Collections;
import java.util.Map;

public class TreeNode {

    private Map<String, PropertyState> properties;
    private String name;
    private String path;

    private Map<String, TreeNode> children;

    public TreeNode(String name, String path, Map<String, TreeNode> children, Map<String, PropertyState> properties) {
        this.properties = properties;
        this.name = name;
        this.children = children;
        this.path = path;
    }

    public Map<String, PropertyState> getProperties() {
        return properties;
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public Map<String, TreeNode> getChildren() {
        return children != null ? children : Collections.emptyMap();
    }

}
