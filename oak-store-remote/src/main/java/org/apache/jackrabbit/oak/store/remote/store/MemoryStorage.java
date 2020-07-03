package org.apache.jackrabbit.oak.store.remote.store;

import org.apache.jackrabbit.oak.api.PropertyState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryStorage implements Storage{
    private static MemoryStorage INSTANCE;

    private AtomicLong currentRevision = new AtomicLong(1);

    public MemoryStorage() {

    }

//    public static MemoryStorage getInstance() {
//        if(INSTANCE == null) {
//            INSTANCE = new MemoryStorage();
//        }
//
//        return INSTANCE;
//    }

    private TreeMap<String, List<Node>> tree = new TreeMap<>();

    public long getCurrentRevision() {
        return currentRevision.get();
    }

    public long incrementRevisionNumber() {
        return currentRevision.incrementAndGet();
    }

    public Node addNode(String path, Map<String, PropertyState> properties, long revision) {
        String name = path.substring(path.lastIndexOf("/") + 1);
        Node node = new Node(name, properties, revision);

        addNode(path, node);
        return node;
    }

    public Node addNode(String path, Iterable<? extends PropertyState> properties, long revision) {
        Map<String, PropertyState> propertiesMap = new HashMap<>();
        for(PropertyState propertyState : properties) {
            propertiesMap.put(propertyState.getName(), propertyState);
        }
        String name = path.substring(path.lastIndexOf("/") + 1);
        Node node = new Node(name, propertiesMap, revision);

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

        SortedMap<String, List<Node>> nodesTree = tree.subMap(path, path + '~');

        for(List<Node> nodeRevisions : nodesTree.values()) {
            Node lastRevision = nodeRevisions.get(nodeRevisions.size() - 1);
            lastRevision.setRevisionDeleted(revision);
        }

//        Node node = getNode(path, revision);
//
//        if (node != null) {
//            node.setRevisionDeleted(revision);
//        }
    }

    public void deleteNode(String path) {
        deleteNode(path, getCurrentRevision());
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

        SortedMap<String, List<Node>> nodesTree = tree.subMap(path, path + '~');

        TreeMap<String, Node> result = new TreeMap<>();

        for(String nodePath : nodesTree.keySet()){
            String matchPath = path.equals("/")? "" : path;
            if (!wholeSubtree && !nodePath.equals(path) && !nodePath.matches(matchPath + "(/[^/]+)?")) {
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

    public void moveChildNodes(String fromPath, String toPath) {
        SortedMap<String, List<Node>> nodesTree = tree.subMap(fromPath, fromPath + '~');

        Map<String, List<Node>> movedNodes = new HashMap<>();

        Iterator<Map.Entry<String, List<Node>>> nodesIterator = nodesTree.entrySet().iterator();
        while(nodesIterator.hasNext()) {
            Map.Entry<String, List<Node>> entry = nodesIterator.next();
            String path = entry.getKey();
            if(path.equals(fromPath)) {
                continue;
            }

            path = path.replace(fromPath, toPath);
            List<Node> revisions = entry.getValue();
            revisions.get(revisions.size() - 1).setRevisionDeleted(Long.MAX_VALUE);
            movedNodes.put(path, revisions);
            nodesIterator.remove();
        }

        tree.putAll(movedNodes);
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

