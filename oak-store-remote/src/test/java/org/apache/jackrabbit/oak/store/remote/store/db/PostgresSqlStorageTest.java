package org.apache.jackrabbit.oak.store.remote.store.db;

import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.store.remote.AbstractRemoteNodeStoreRepoTest;
import org.apache.jackrabbit.oak.store.remote.RemoteNodeStore;
import org.apache.jackrabbit.oak.store.remote.RemoteNodeStoreRepoTest;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.db.ConnectionPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PostgresSqlStorageTest {


    @ClassRule
    public static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer("postgres:12.3");

    private Connection dbConnection;

    private DbStorage dbStorage;

    private static final String TABLE = "persistence1.nodes";

    @BeforeClass
    public static void beforeClass() {
        postgreSQLContainer.start();
    }

    @Before
    public void setup() throws SQLException, RepositoryException, ParseException, IOException {

        Properties props = new Properties();
        props.setProperty("user", postgreSQLContainer.getUsername());
        props.setProperty("password", postgreSQLContainer.getPassword());

        ConnectionPool connectionPool = () -> DriverManager.getConnection(postgreSQLContainer.getJdbcUrl(), props);

        dbConnection = connectionPool.getConnection();
        initDb();

        dbStorage = new DbStorage(connectionPool, TABLE);

    }

    private void initDb() throws SQLException {
        Statement statement = dbConnection.createStatement();

        statement.execute("" +
                "CREATE SCHEMA persistence1;\n" +
                "" +
                "CREATE TABLE "+TABLE+"\n" +
                "(\n" +
                "    path character varying COLLATE pg_catalog.\"default\" NOT NULL,\n" +
                "    parent_path character varying COLLATE pg_catalog.\"default\" NOT NULL,\n" +
                "    revision bigint NOT NULL,\n" +
                "    revision_deleted bigint,\n" +
                "    properties jsonb,\n" +
                "    CONSTRAINT nodes_pkey PRIMARY KEY (path, revision)\n" +
                ")\n" +
                "\n" +
                "TABLESPACE pg_default;\n" +
                "\n" +
                "\n" +
                "CREATE INDEX path" +
                "    ON "+TABLE+" USING btree" +
                "    (path COLLATE pg_catalog.\"default\" ASC NULLS LAST)" +
                //"    INCLUDE(revision, revision_deleted, properties)" +
                "    TABLESPACE pg_default;" +
                "\n" +
                "CREATE INDEX parent_path" +
                "   ON "+TABLE+" USING btree" +
                "   (path COLLATE pg_catalog.\"default\" ASC NULLS LAST)"+
                "   TABLESPACE pg_default;");
    }

    @After
    public void tearDown() throws SQLException {
        Statement statement = dbConnection.createStatement();
        statement.execute("DROP SCHEMA persistence1 CASCADE;");
        dbConnection.close();
    }

    @AfterClass
    public static void afterClass() {
        postgreSQLContainer.stop();
    }

    @Test
    public void testSimple() throws SQLException {
        Statement statement = dbConnection.createStatement();
        statement.executeQuery("SELECT 1");

    }

    @Test
    public void testAddNode() throws SQLException {
        List<PropertyState> props = new ArrayList<>();
        PropertyState prop1 = PropertyStates.createProperty("prop1", "val1", PropertyType.STRING);
        PropertyState prop2 = PropertyStates.createProperty("prop2", "5", PropertyType.LONG);
        List<String> strings = new ArrayList<>();
        strings.add("elem1");
        strings.add("elem2");
        strings.add("elem3");
        PropertyState prop3 = PropertyStates.createProperty("prop3", (Object) strings, Type.STRINGS);

        props.add(prop1);
        props.add(prop2);
        props.add(prop3);

        dbStorage.addNode("/a", props);

        Statement statement = dbConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM "+TABLE);

        assertTrue(resultSet.first());
        assertEquals("/a", resultSet.getString("path"));
        assertEquals("/", resultSet.getString("parent_path"));
        Map<String, PropertyState> propertyMap = dbStorage.deserializeProperties(resultSet.getString("properties"));
        assertEquals(prop1, propertyMap.get("prop1"));
        assertEquals(prop2, propertyMap.get("prop2"));
        assertEquals(prop3, propertyMap.get("prop3"));
        assertFalse(resultSet.next());

        dbStorage.addNode("/a/b/c", Collections.emptyList());

        resultSet = statement.executeQuery("SELECT * FROM "+TABLE+" WHERE path = '/a/b/c'");
        assertTrue(resultSet.first());
        assertEquals("/a/b/c", resultSet.getString("path"));
        assertEquals("/a/b", resultSet.getString("parent_path"));
    }

    @Test
    public void getNodeWithProperties() throws SQLException {
        List<PropertyState> props = new ArrayList<>();
        PropertyState prop1 = PropertyStates.createProperty("prop1", "val1", PropertyType.STRING);
        PropertyState prop2 = PropertyStates.createProperty("prop2", "5", PropertyType.LONG);
        List<String> strings = new ArrayList<>();
        strings.add("elem1");
        strings.add("elem2");
        strings.add("elem3");
        PropertyState prop3 = PropertyStates.createProperty("prop3", (Object) strings, Type.STRINGS);

        props.add(prop1);
        props.add(prop2);
        props.add(prop3);

        String insertStmtString = "INSERT INTO "+TABLE+" (path, revision, revision_deleted, parent_path, properties) VALUES (?, ?, ?, ?, ?::JSON)";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(insertStmtString);

        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 1);
        preparedStatement.setNull(3, Types.BIGINT);
        preparedStatement.setString(4, "/a");
        String propsSerialized = dbStorage.serializeProperties(props);
        preparedStatement.setString(5, propsSerialized);

        preparedStatement.execute();

        Node node = dbStorage.getNode("/a/b", 1);
        assertEquals(prop1, node.getProperties().get("prop1"));
        assertEquals(prop2, node.getProperties().get("prop2"));
        assertEquals(prop3, node.getProperties().get("prop3"));

    }

    @Test
    public void testGetNode() throws SQLException {
        String insertStmtString = "INSERT INTO "+TABLE+" (path, revision, revision_deleted, parent_path) VALUES (?, ?, ?, ?)";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(insertStmtString);

        //add the first node
        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 1);
        preparedStatement.setNull(3, Types.BIGINT);
        preparedStatement.setString(4, "/a");

        preparedStatement.execute();

        Node node = dbStorage.getNode("/a/b", 1);

        assertNotNull(node);

        //add second node
        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 2);
        preparedStatement.setNull(3, Types.BIGINT);
        preparedStatement.setString(4, "/a");

        preparedStatement.execute();

        node = dbStorage.getNode("/a/b", 2);

        assertNotNull(node);
        assertEquals("b", node.getName());
        assertEquals(2, node.getRevision());

        //add third revision but mark as deleted
        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 3);
        preparedStatement.setLong(3, 4);
        preparedStatement.setString(4, "/a");

        preparedStatement.execute();

        node = dbStorage.getNode("/a/b", 5);
        assertNull(node);

        node = dbStorage.getNode("/a/b", 4);
        assertNull(node);
    }

    @Test
    public void testDeleteNode() throws SQLException {
        String insertStmtString = "INSERT INTO "+TABLE+" (path, revision, parent_path) VALUES (?, ?, ?)";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(insertStmtString);

        //add the first node
        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 1);
        preparedStatement.setString(3, "/a");
        preparedStatement.execute();

        //add second revision
        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 2);
        preparedStatement.setString(3, "/a");
        preparedStatement.execute();

        dbStorage.deleteNode("/a/b", 3);

        Statement statement = dbConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM "+TABLE+" ORDER BY revision DESC");

        assertTrue(resultSet.first());

        assertEquals("/a/b", resultSet.getString("path"));
        assertEquals(2, resultSet.getLong("revision"));
        assertEquals(3, resultSet.getLong("revision_deleted"));

        assertTrue(resultSet.next());

        assertEquals("/a/b", resultSet.getString("path"));
        assertEquals(1, resultSet.getLong("revision"));
        assertEquals(0, resultSet.getLong("revision_deleted"));

        assertFalse(resultSet.next());
    }

    @Test
    public void testGetNodeAndSubtree() throws SQLException {
        String insertStmtString = "INSERT INTO "+TABLE+" (path, revision, revision_deleted, parent_path) VALUES (?, ?, ?, ?)";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(insertStmtString);

        preparedStatement.setString(1, "/a");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 2);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/d");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a/b");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/d/e");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a/b/d");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/d");
        preparedStatement.setLong(2, 2);
        preparedStatement.setLong(3, 3);
        preparedStatement.setString(4, "/a/b");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/d/e");
        preparedStatement.setLong(2, 2);
        preparedStatement.setLong(3, 3);
        preparedStatement.setString(4, "/a/b/d");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/c");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/f");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a/b");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/f/f1");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a/b/f");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/f/f2");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a/b/f");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/f/f2");
        preparedStatement.setLong(2, 2);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a/b/f");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/g");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a/b");
        preparedStatement.execute();

        TreeMap<String, Node> result =  dbStorage.getNodeAndSubtree("/a/b", 5, true);

        assertFalse(result.isEmpty());

        /*
        result should contain
            /a/b
            /a/b/f
            /a/b/f/f1
            /a/b/f/f2
            /a/b/g
         */
        assertEquals(5, result.size());
        assertNotNull(result.get("/a/b"));
        assertEquals(2, result.get("/a/b").getRevision());
        assertNotNull(result.get("/a/b/f"));
        assertNotNull(result.get("/a/b/f/f1"));
        assertNotNull(result.get("/a/b/f/f2"));
        assertEquals(2, result.get("/a/b/f/f2").getRevision());
        assertNotNull(result.get("/a/b/g"));

        //delete f2
        preparedStatement.setString(1, "/a/b/f/f2");
        preparedStatement.setLong(2, 3);
        preparedStatement.setLong(3, 3);
        preparedStatement.setString(4, "/a/b/f");
        preparedStatement.execute();

        result =  dbStorage.getNodeAndSubtree("/a/b", 5, true);
        assertEquals(4, result.size());
        assertNull(result.get("/a/b/f/f2"));

        //test fetching just direct child nodes
        result =  dbStorage.getNodeAndSubtree("/a/b", 5, false);

        assertEquals(3, result.size());
        assertNotNull(result.get("/a/b"));
        assertEquals(2, result.get("/a/b").getRevision());
        assertNotNull(result.get("/a/b/f"));
        assertNotNull(result.get("/a/b/g"));
    }

    @Test
    public void testMoveChildNodes() throws SQLException {
        String insertStmtString = "INSERT INTO "+TABLE+" (path, revision, revision_deleted, parent_path) VALUES (?, ?, ?, ?)";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(insertStmtString);

        preparedStatement.setString(1, "/a");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 1);
//        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setObject(3, 1);
        preparedStatement.setString(4, "/a");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/d");
        preparedStatement.setLong(2, 1);
        //preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setObject(3, 1);
        preparedStatement.setString(4, "/a/b");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/b/c");
        preparedStatement.setLong(2, 1);
        //preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setObject(3, 1);
        preparedStatement.setString(4, "/a/b");
        preparedStatement.execute();

        preparedStatement.setString(1, "/a/e");
        preparedStatement.setLong(2, 1);
        preparedStatement.setObject(3, null, Types.BIGINT);
        preparedStatement.setString(4, "/a");
        preparedStatement.execute();

        dbStorage.moveChildNodes("/a/b", "/a/e");

        TreeMap<String, Node> tree = dbStorage.getNodeAndSubtree("/a/e", 5, true);

        assertEquals(3, tree.size());
        assertNotNull(tree.get("/a/e/d"));
        assertNotNull(tree.get("/a/e/c"));
    }

    @Test
    public void testGetRootNode() throws SQLException {

        dbStorage.addNode("/", Collections.emptyList());
        dbStorage.incrementRevisionNumber();

        dbStorage.addNode("/", Collections.emptyList());
        dbStorage.incrementRevisionNumber();

        dbStorage.addNode("/", Collections.emptyList());
        dbStorage.incrementRevisionNumber();

        Node root = dbStorage.getRootNode();

        assertNotNull(root);
        assertEquals(3, root.getRevision());
    }
}
