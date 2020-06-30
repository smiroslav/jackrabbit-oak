package org.apache.jackrabbit.oak.store.remote.store;

import org.apache.jackrabbit.oak.api.PropertyState;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemoryStorage {
    private static MemoryStorage INSTANCE;

    private MemoryStorage() {

    }

    public static MemoryStorage getInstance() {
        if(INSTANCE == null) {
            INSTANCE = new MemoryStorage();
        }

        return INSTANCE;
    }

    private TreeMap<String, Node> tree = new TreeMap<>();

    public Node addNode(String path, Map<String, PropertyState> properties) {
        String name = path.substring(path.lastIndexOf("/"));
        Node node = new Node(name, properties);
        tree.put(path, node);
        return node;
    }

    public Node addNode(String path, Node node) {
        tree.put(path, node);
        return node;
    }

    public Node getNode(String path) {
        return tree.get(path);
    }

    public SortedMap<String, Node> getNodeAndSubtree(String path) {
        return tree.subMap(path, path + 1);
    }

    public static class Node {
        private Map<String, PropertyState> properties;
        private String name;

        public Node(String name, Map<String, PropertyState> properties) {
            this.properties = properties;
            this.name = name;
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

    }

    public static void main(String[] args) {
        TreeMap<String, String> map = new TreeMap();

        map.put("/content", "content");
        map.put("/content/a", "a");
        map.put("/content/aa", "aa");
        map.put("/content/a/a1", "a1");
        map.put("/content/a/a1/a2", "a2");
        map.put("/content/a/a11", "a11");
        map.put("/content/b", "b");
        map.put("/content/b/b1", "b1");
        map.put("/content/c", "c");
        map.put("/content/c/c1", "c1");

        //SortedMap submap = map.tailMap("/content/a");

        SortedMap<String, String> submap = map.subMap("/content/a", "/content/a" + 1);

        System.out.println("subtree of /content/a:\n");
        for(String key : submap.keySet()) {
            System.out.println(key);
        }

        System.out.println("\n\ndirect child nodes of /content/a:\n");
        for(String key : submap.keySet()) {
            if (key.matches("/content/a/[^/]+")) {
                System.out.println(key);
            }
        }


    }
}

