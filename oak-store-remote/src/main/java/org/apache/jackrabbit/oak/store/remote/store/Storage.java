package org.apache.jackrabbit.oak.store.remote.store;

import org.apache.jackrabbit.oak.api.PropertyState;

import java.util.Map;
import java.util.TreeMap;

public interface Storage {

    Node addNode(String path, Map<String, PropertyState> properties);

    Node addNode(String path, Iterable<? extends PropertyState> properties);

    Node addNode(String path, Node node);

    void deleteNode(String path, long revision);

    void deleteNode(String path);

    Node getNode(String path, long revision);

    Node getRootNode();

    TreeMap<String, Node> getNodeAndSubtree(String path, long revision, boolean wholeSubtree);

    void moveChildNodes(String fromPath, String toPath);

    long incrementRevisionNumber();

    long getCurrentRevision();
}
