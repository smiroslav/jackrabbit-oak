package org.apache.jackrabbit.oak.store.remote.store.tinkerpop;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TinkerPopStorage implements AutoCloseable {

    private final Cluster cluster;

    private final GraphTraversalSource g;

    public TinkerPopStorage(int port) {
        this.cluster = Cluster.build().port(port).create();
        this.g = traversal().withRemote(DriverRemoteConnection.using(cluster));
    }


    @Override
    public void close() throws Exception {
        cluster.close();
    }
}
