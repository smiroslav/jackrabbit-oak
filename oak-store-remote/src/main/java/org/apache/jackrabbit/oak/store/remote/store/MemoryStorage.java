package org.apache.jackrabbit.oak.store.remote.store;

import org.apache.jackrabbit.oak.api.PropertyState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryStorage {
    private static MemoryStorage INSTANCE;

    private AtomicLong currentRevision = new AtomicLong(0);

    private MemoryStorage() {

    }

    public static MemoryStorage getInstance() {
        if(INSTANCE == null) {
            INSTANCE = new MemoryStorage();
        }

        return INSTANCE;
    }

    private TreeMap<String, List<Node>> tree = new TreeMap<>();

    public long getCurrentRevision() {
        return currentRevision.get();
    }

    public Node addNode(String path, Map<String, PropertyState> properties, long revision) {
        String name = path.substring(path.lastIndexOf("/") + 1);
        Node node = new Node(name, properties, revision);

        addNode(path, node);
        return node;
    }

    public Node addNode(String path, Node node) {
        List<Node> nodes = tree.get(path);
        if (nodes == null) {
            nodes = new ArrayList<>();
        }
        nodes.add(node);
        tree.put(path, nodes);

        return node;
    }

    public void deleteNode(String path, long revision) {
        Node node = getNode(path, revision);

        if (node != null) {
            node.setRevisionDeleted(revision);
        }
    }

    public Node getNode(String path, long revision) {
        Node result = null;
        List<Node> nodes = tree.get(path);
        if (nodes != null) {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                Node node = nodes.get(i);
                if(node.existsForRevision(revision)) {
                    result = node;
                    break;
                }
            }
        }

        return result;
    }

    public Node getRootNode() {
        Node node = null;
        List<Node> nodes = tree.get("/");
        if (nodes != null) {
            node = nodes.get(nodes.size() - 1);
        }
        return node;
    }

    public TreeMap<String, Node> getNodeAndSubtree(String path, long revision, boolean wholeSubtree) {
        TreeMap<String, Node> result = new TreeMap<>();

        SortedMap<String, List<Node>> nodesTree = tree.subMap(path, path + 1);

        for(String nodePath : nodesTree.keySet()){
            if (!nodePath.matches(path + "(/[^/]+)?")) {
                continue;
            }
            List<Node> nodes = nodesTree.get(nodePath);
            for (int i = nodes.size() - 1; i >= 0; i--) {
                Node node = nodes.get(i);
                if(node.existsForRevision(revision)) {
                    result.put(nodePath, node);
                    break;
                }
            }
        }

        return result;
    }

    public static class Node {
        private Map<String, PropertyState> properties;
        private String name;
        private long revision;
        private long revisionDeleted = Long.MAX_VALUE;

        public Node(String name, Map<String, PropertyState> properties, long revision) {
            this.properties = properties != null ? properties : Collections.emptyMap();
            this.name = name;
            this.revision = revision;
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

    public static void main(String[] args) {
        TreeMap<String, String> map = new TreeMap();

        map.put("/content", "content");
        map.put("/content/a", "a");
        map.put("/content/aa", "aa");
        map.put("/content/a/a1", "a1");
        map.put("/content/a/a1", "_a1");
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
            System.out.println(key+" : "+submap.get(key));
        }

        System.out.println("\n\ndirect child nodes of /content/a:\n");
        for(String key : submap.keySet()) {
            if (key.matches("/content/a/[^/]+")) {
                System.out.println(key+" : "+submap.get(key));
            }
        }


    }
}

