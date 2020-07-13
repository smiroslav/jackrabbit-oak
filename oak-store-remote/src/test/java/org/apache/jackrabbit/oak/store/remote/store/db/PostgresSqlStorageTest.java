package org.apache.jackrabbit.oak.store.remote.store.db;

import org.apache.jackrabbit.oak.store.remote.store.db.ConnectionPool;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PostgresSqlStorageTest{


    @Rule
    public PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer();
    private Connection dbConnection;

    private DbStorage dbStorage;

    private static final String TABLE = "persistence1.nodes";

    @Before
    public void setup() throws SQLException {
        postgreSQLContainer.start();

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
                "    TABLESPACE pg_default;");
    }

    @After
    public void tearDown() {
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
        assertFalse(resultSet.next());
    }
}
