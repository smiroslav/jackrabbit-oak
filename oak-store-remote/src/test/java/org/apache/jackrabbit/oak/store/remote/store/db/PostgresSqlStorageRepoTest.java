package org.apache.jackrabbit.oak.store.remote.store.db;

import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.store.remote.AbstractRemoteNodeStoreRepoTest;
import org.apache.jackrabbit.oak.store.remote.RemoteNodeStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
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

public class PostgresSqlStorageRepoTest extends AbstractRemoteNodeStoreRepoTest {


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

        fileBlobStore = new FileBlobStore(blobStoreDir.getRoot().getAbsolutePath());

        nodeStore = new RemoteNodeStore(dbStorage, fileBlobStore);

        super.setUp();
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

}
