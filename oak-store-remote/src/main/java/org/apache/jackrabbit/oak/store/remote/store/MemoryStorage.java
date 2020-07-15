package org.apache.jackrabbit.oak.store.remote.store;

import org.apache.jackrabbit.oak.api.PropertyState;

import java.util.ArrayList;
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

    private TreeMap<String, List<Node>> tree = new TreeMap<>();

    public long getCurrentRevision() {
        return currentRevision.get();
    }

    public long incrementRevisionNumber() {
        return currentRevision.incrementAndGet();
    }

//    public void addNode(String path, Map<String, PropertyState> properties) {
//        String name = path.substring(path.lastIndexOf("/") + 1);
//        Node node = new Node(name, properties, getCurrentRevision());
//
//        addNode(path, node);
//    }

    public void addNode(String path, Iterable<? extends PropertyState> properties) {
        Map<String, PropertyState> propertiesMap = new HashMap<>();
        for(PropertyState propertyState : properties) {
            propertiesMap.put(propertyState.getName(), propertyState);
        }
        String name = path.substring(path.lastIndexOf("/") + 1);
        Node node = new Node(name, propertiesMap, getCurrentRevision());

        addNode(path, node);
    }

    public void addNode(String path, Node node) {
        List<Node> nodes = tree.get(path);
        if (nodes == null) {
            nodes = new ArrayList<>();
        }
        nodes.add(node);
        tree.put(path, nodes);
    }

    public void deleteNode(String path, long revision) {

        SortedMap<String, List<Node>> nodesTree = tree.subMap(path, path + '~');

        for(List<Node> nodeRevisions : nodesTree.values()) {
            Node lastRevision = nodeRevisions.get(nodeRevisions.size() - 1);
            lastRevision.setRevisionDeleted(revision);
        }
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
            Node lastRevision = revisions.get(revisions.size() - 1);
            Node lastRevisionClone = new Node(lastRevision.getName(), lastRevision.getProperties(), getCurrentRevision());
            List<Node> movedNodeRevisions = new ArrayList<>();
            movedNodeRevisions.add(lastRevisionClone);
            movedNodes.put(path, movedNodeRevisions);
        }

        tree.putAll(movedNodes);
    }
}

