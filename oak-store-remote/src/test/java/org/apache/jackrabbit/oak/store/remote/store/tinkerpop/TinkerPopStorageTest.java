package org.apache.jackrabbit.oak.store.remote.store.tinkerpop;

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TinkerPopStorageTest {

    private static final String CONFIG = "org/apache/jackrabbit/oak/store/remote/store/tinkerpop/gremlin-server.yaml";

    private GremlinServer server;

    @Before
    public void setUp() throws Exception {
        startServer();
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
        stopServer();
    }

    /**
     * Stops a current instance of Gremlin Server.
     */
    public void stopServer() throws Exception {
        server.stop().join();
    }



    @Test
    public void shouldCreateGraph() throws Exception {

    }
}
