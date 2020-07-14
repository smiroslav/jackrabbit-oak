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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class DbStorage implements Storage {

    private final ConnectionPool connectionPool;

    private final String tableName;
    private final String insertStmtString;
    private final String getNodeStmtString;
    private final String setDeletedRevisionStmtString;
    private final String getSubtreeStmtString;
    private final AtomicLong currentRevision;

    public DbStorage(ConnectionPool connectionPool, String tableName) {
        this.connectionPool = connectionPool;
        this.tableName = tableName;
        insertStmtString = "INSERT INTO "+tableName+" (path, revision, properties) VALUES (?, ?, ?)";
        getNodeStmtString = " SELECT path, revision, revision_deleted, properties FROM "+tableName+" " +
                            //" WHERE path = ? AND (revision_deleted IS NULL OR revision_deleted > ?) " +
                            " WHERE path = ? AND (revision <= ?) " +
                            " ORDER BY revision DESC";
        setDeletedRevisionStmtString = " UPDATE "+tableName+" SET revision_deleted = ? " +
                                       " WHERE path = ? AND revision = ?";
        getSubtreeStmtString = "" +
                "SELECT a.path, a.revision FROM "+tableName+" a\n" +
                "INNER JOIN (\n" +
                "\tSELECT path, MAX(revision) as revision\n" +
                "    FROM persistence1.nods\n" +
                "\tWHERE path >= '/a/b' AND path < '/a/b~'\n" +
                "    GROUP BY path\n" +
                ") as b ON a.path = b.path AND a.revision = b.revision\n" +
                "WHERE a.path >= '/a/b' AND a.path < '/a/b~' AND a.revision_deleted IS NULL AND a.revision <= 3"
        try {
            currentRevision  = new AtomicLong(getCurrentRevisionFromTable());
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    private long getCurrentRevisionFromTable() throws SQLException {
        try (Connection connection = connectionPool.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT MAX(revision) FROM "+tableName);
            if (resultSet.next()) {
                return  resultSet.getLong(1);
            } else {
                return -1;
            }
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
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement insertStmt = connection.prepareStatement(insertStmtString);
            insertStmt.setString(1, path);
            insertStmt.setLong(2, getCurrentRevision());
            insertStmt.setString(3, serializeProperties(properties));

            insertStmt.execute();
        }
    }

    private String serializeProperties(Iterable<? extends PropertyState> properties) {
        //TODO
        return "";
    }

    @Override
    public void addNode(String path, Node node) {
        addNode(path, node.getProperties().values());
    }

    @Override
    public void deleteNode(String path, long revision) {
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(getNodeStmtString, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setString(1, path);
            preparedStatement.setLong(2, revision);

            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.first()) {
                String nodePath = resultSet.getString(1);
                Long nodeRevision = resultSet.getLong(2);

                PreparedStatement updateStatement = connection.prepareStatement(setDeletedRevisionStmtString);
                updateStatement.setLong(1, revision);
                updateStatement.setString(2, nodePath);
                updateStatement.setLong(3, nodeRevision);

                updateStatement.execute();
            }

        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteNode(String path) {
        deleteNode(path, getCurrentRevision());
    }

    @Override
    public Node getNode(String path, long revision) {
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(getNodeStmtString, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setString(1, path);
            preparedStatement.setLong(2, revision);

            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.first()) {
                String nodePath = resultSet.getString(1);
                Long nodeRevision = resultSet.getLong(2);
                Long nodeRevisionDeleted = resultSet.getLong(3);
                if (nodeRevision <= revision && (nodeRevisionDeleted == null || nodeRevisionDeleted == 0)) {
                    Map<String, PropertyState> propertyStates = deserializeProperties(resultSet.getString(4));
                    return new Node(nodePath.substring(nodePath.lastIndexOf('/') + 1), propertyStates, nodeRevision);
                }
            }

            return null;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    private Map<String, PropertyState> deserializeProperties(String properties) {
        //TODO
        return Collections.emptyMap();
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
