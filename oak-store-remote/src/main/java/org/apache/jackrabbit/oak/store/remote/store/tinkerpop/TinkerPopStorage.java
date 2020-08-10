package org.apache.jackrabbit.oak.store.remote.store.tinkerpop;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.Storage;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

public class TinkerPopStorage implements Storage {

    private final GraphTraversalSource g;

    public TinkerPopStorage(GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public void addNode(String path, Iterable<? extends PropertyState> properties) {
    }

    @Override
    public void addNode(String path, Node node) {

    }

    @Override
    public void deleteNode(String path, long revision) {

    }

    @Override
    public void deleteNode(String path) {

    }

    @Override
    public Node getNode(String path, long revision) {
        return null;
    }

    @Override
    public Node getRootNode() {
        List<Vertex> list = g.V().hasLabel("root").toList();

        if (list.isEmpty()) {
            return null;
        }

        long revision = (long) g.V().hasLabel("root").elementMap().next().get("revision");
        Node rootNode = new Node("/", Collections.emptyMap(), revision);
        return rootNode;
    }

    @Override
    public TreeMap<String, Node> getNodeAndSubtree(String path, long revision, boolean wholeSubtree) {
        return null;
    }

    @Override
    public void moveChildNodes(String fromPath, String toPath) {

    }

    @Override
    public long incrementRevisionNumber() {
        return 0;
    }

    @Override
    public long getCurrentRevision() {
        return 0;
    }
}
