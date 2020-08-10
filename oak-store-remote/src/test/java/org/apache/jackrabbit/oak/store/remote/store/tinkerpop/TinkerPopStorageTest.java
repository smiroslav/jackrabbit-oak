package org.apache.jackrabbit.oak.store.remote.store.tinkerpop;

import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class TinkerPopStorageTest {

    private static final String CONFIG = "org/apache/jackrabbit/oak/store/remote/store/tinkerpop/gremlin-server.yaml";

    private TinkerPopStorage storage;

    private GremlinServer server;

    private Cluster cluster;
    private GraphTraversalSource g;

    @Before
    public void setUp() throws Exception {
        startServer();

        cluster = Cluster.build().port(45940).create();

        g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        storage = new TinkerPopStorage(g);
    }

    /**
     * Starts a new instance of Gremlin Server.
     */
    public void startServer() throws Exception {
        final InputStream stream = getClass().getClassLoader().getResourceAsStream(CONFIG);
        this.server = new GremlinServer(Settings.read(stream));

        server.start().join();
    }

    @After
    public void tearDown() throws Exception {
        cluster.close();
        server.stop().join();
    }


    @Test
    public void testGerRootNode() throws Exception {
        Node node = storage.getRootNode();

        assertNull(node);

        g.addV("root").property("revision", 1l).next();
        g.addV("root").property("revision", 2l).next();

        node = storage.getRootNode();

        assertNotNull(node);
    }
}
