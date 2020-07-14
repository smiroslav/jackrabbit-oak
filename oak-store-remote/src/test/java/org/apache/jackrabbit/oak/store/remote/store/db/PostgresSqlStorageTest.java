package org.apache.jackrabbit.oak.store.remote.store.db;

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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.Properties;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PostgresSqlStorageTest{


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
    public void setup() throws SQLException {

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
                "    properties character varying COLLATE pg_catalog.\"default\",\n" +
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
        dbStorage.addNode("/a", Collections.emptyList());

        Statement statement = dbConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM "+TABLE);

        assertTrue(resultSet.first());
        assertEquals("/a", resultSet.getString("path"));
        assertEquals("/", resultSet.getString("parent_path"));
        assertFalse(resultSet.next());

        dbStorage.addNode("/a/b/c", Collections.emptyList());

        resultSet = statement.executeQuery("SELECT * FROM "+TABLE+" WHERE path = '/a/b/c'");
        assertTrue(resultSet.first());
        assertEquals("/a/b/c", resultSet.getString("path"));
        assertEquals("/a/b", resultSet.getString("parent_path"));
    }

    @Test
    public void testGetNode() throws SQLException {
        String insertStmtString = "INSERT INTO "+TABLE+" (path, revision, revision_deleted) VALUES (?, ?, ?)";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(insertStmtString);

        //add the first node
        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 1);
        preparedStatement.setNull(3, Types.BIGINT);

        preparedStatement.execute();

        Node node = dbStorage.getNode("/a/b", 1);

        assertNotNull(node);

        //add second node
        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 2);
        preparedStatement.setNull(3, Types.BIGINT);

        preparedStatement.execute();

        node = dbStorage.getNode("/a/b", 2);

        assertNotNull(node);
        assertEquals("b", node.getName());
        assertEquals(2, node.getRevision());

        //add third revision but mark as deleted
        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 3);
        preparedStatement.setLong(3, 4);

        preparedStatement.execute();

        node = dbStorage.getNode("/a/b", 5);
        assertNull(node);

        node = dbStorage.getNode("/a/b", 4);
        assertNull(node);
    }

    @Test
    public void testDeleteNode() throws SQLException {
        String insertStmtString = "INSERT INTO "+TABLE+" (path, revision) VALUES (?, ?)";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(insertStmtString);

        //add the first node
        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 1);
        preparedStatement.execute();

        //add second revision
        preparedStatement.setString(1, "/a/b");
        preparedStatement.setLong(2, 2);
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
}
