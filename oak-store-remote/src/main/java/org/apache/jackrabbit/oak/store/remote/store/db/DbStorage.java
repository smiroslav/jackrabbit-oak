package org.apache.jackrabbit.oak.store.remote.store.db;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.Storage;
import org.apache.jackrabbit.oak.store.remote.store.StorageException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class DbStorage implements Storage {

    private final ConnectionPool connectionPool;

    private final String tableName;
    private final String insertStmtString;
    private final AtomicLong currentRevision;

    public DbStorage(ConnectionPool connectionPool, String tableName) {
        this.connectionPool = connectionPool;
        this.tableName = tableName;
        insertStmtString = "INSERT INTO "+tableName+" (path, revision, properties) VALUES (?, ?, ?)";
        try {
            currentRevision  = new AtomicLong(getCurrentRevisionFromTable());
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    private long getCurrentRevisionFromTable() throws SQLException {
        Connection connection = connectionPool.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT MAX(revision) FROM "+tableName);
        if (resultSet.next()) {
            return  resultSet.getLong(1);
        } else {
            return -1;
        }
    }

    @Override
    public void addNode(String path, Iterable<? extends PropertyState> properties) {

        try {
            insertNewNode(path, getCurrentRevision(), properties);
        } catch (SQLException e) {
            new StorageException(e);
        }
    }

    private void insertNewNode(String path, long currentRevision, Iterable<? extends PropertyState> properties) throws SQLException {
        Connection connection = connectionPool.getConnection();
        PreparedStatement insertStmt = connection.prepareStatement(insertStmtString);
        insertStmt.setString(1, path);
        insertStmt.setLong(2, getCurrentRevision());
        insertStmt.setString(3, serializeProperties(properties));

        insertStmt.execute();
    }

    private String serializeProperties(Iterable<? extends PropertyState> properties) {
        return "";
    }

    @Override
    public void addNode(String path, Node node) {
        addNode(path, node.getProperties().values());
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
        return null;
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
        return 1;
    }
}
