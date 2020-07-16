package org.apache.jackrabbit.oak.store.remote.store.db;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DecimalPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiBooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiDecimalPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiDoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiLongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.Storage;
import org.apache.jackrabbit.oak.store.remote.store.StorageException;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class DbStorage implements Storage {

    private final ConnectionPool connectionPool;

    private final String tableName;
    private final String insertStmtString;
    private final String getNodeStmtString;
    private final String setDeletedRevisionStmtString;
    private final String getSubtreeStmtString;
    private final String getChildNodesStmtString;
    private final AtomicLong currentRevision;

    private Gson gson;

    public DbStorage(ConnectionPool connectionPool, String tableName) {
        this.connectionPool = connectionPool;
        this.tableName = tableName;
        insertStmtString = "INSERT INTO "+tableName+" (path, revision, properties, parent_path) VALUES (?, ?, ?, ?)";
        getNodeStmtString = " SELECT path, revision, revision_deleted, properties FROM "+tableName+" " +
                            //" WHERE path = ? AND (revision_deleted IS NULL OR revision_deleted > ?) " +
                            " WHERE path = ? AND (revision <= ?) " +
                            " ORDER BY revision DESC";
        setDeletedRevisionStmtString = " UPDATE "+tableName+" SET revision_deleted = ? " +
                                       " WHERE path = ? AND revision = ?";
        getSubtreeStmtString = "" +
                "SELECT a.path, a.revision, a.properties FROM "+tableName+" a\n" +
                "INNER JOIN (\n" +
                "  SELECT path, MAX(revision) as revision\n" +
                "  FROM "+tableName+"\n" +
                "  WHERE path = ? OR path LIKE ?\n" +
                "  GROUP BY path\n" +
                ") as b ON a.path = b.path AND a.revision = b.revision\n" +
                "WHERE (a.path = ? OR a.path LIKE ?) AND (a.revision_deleted IS NULL OR a.revision_deleted = 0) AND a.revision <= ? " +
                "ORDER BY a.path ";

        getChildNodesStmtString = "" +
                "SELECT a.path, a.revision, a.properties FROM "+tableName+" a\n" +
                "INNER JOIN (\n" +
                "  SELECT path, MAX(revision) as revision\n" +
                "  FROM "+tableName+"\n" +
                "  WHERE path = ? OR parent_path = ?\n" +
                "  GROUP BY path\n" +
                ") as b ON a.path = b.path AND a.revision = b.revision\n" +
                "WHERE (a.path = ? OR parent_path = ?) AND (a.revision_deleted IS NULL OR a.revision_deleted = 0) AND a.revision <= ? " +
                "ORDER BY a.path ";

        try {
            currentRevision  = new AtomicLong(getCurrentRevisionFromTable() + 1);
        } catch (SQLException e) {
            throw new StorageException(e);
        }

        initPropertySerializer();
    }

    private void initPropertySerializer() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        PropertyStateSerializer propertyStateSerializer = new PropertyStateSerializer();
        PropertyStateDeserializer propertyStateDeserializer = new PropertyStateDeserializer();

        gsonBuilder.registerTypeAdapter(PropertyState.class, new PropertyStateInstanceCreator());

        //single
        gsonBuilder.registerTypeAdapter(StringPropertyState.class, propertyStateSerializer);
        gsonBuilder.registerTypeAdapter(LongPropertyState.class, propertyStateSerializer);
        gsonBuilder.registerTypeAdapter(BinaryPropertyState.class, propertyStateSerializer);
        gsonBuilder.registerTypeAdapter(DoublePropertyState.class, propertyStateSerializer);
        gsonBuilder.registerTypeAdapter(BooleanPropertyState.class, propertyStateSerializer);
        gsonBuilder.registerTypeAdapter(DecimalPropertyState.class, propertyStateSerializer);
        //multi
        gsonBuilder.registerTypeAdapter(MultiStringPropertyState.class, propertyStateSerializer);
        gsonBuilder.registerTypeAdapter(MultiBinaryPropertyState.class, propertyStateSerializer);
        gsonBuilder.registerTypeAdapter(MultiLongPropertyState.class, propertyStateSerializer);
        gsonBuilder.registerTypeAdapter(MultiDoublePropertyState.class, propertyStateSerializer);
        gsonBuilder.registerTypeAdapter(MultiBooleanPropertyState.class, propertyStateSerializer);
        gsonBuilder.registerTypeAdapter(MultiDecimalPropertyState.class, propertyStateSerializer);

        //single
        gsonBuilder.registerTypeAdapter(PropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(StringPropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(LongPropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(BinaryPropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(DoublePropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(BooleanPropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(DecimalPropertyState.class, propertyStateDeserializer);
        //multi
        gsonBuilder.registerTypeAdapter(MultiStringPropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(MultiBinaryPropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(MultiLongPropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(MultiDoublePropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(MultiBooleanPropertyState.class, propertyStateDeserializer);
        gsonBuilder.registerTypeAdapter(MultiDecimalPropertyState.class, propertyStateDeserializer);

        this.gson = gsonBuilder.create();
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

        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement insertStmt = connection.prepareStatement(insertStmtString);
            insertNewNode(path, properties, insertStmt);
        } catch (SQLException e) {
            new StorageException(e);
        }
    }

    private void insertNewNode(String path, Iterable<? extends PropertyState> properties, PreparedStatement insertStmt) throws SQLException {
        insertStmt.setString(1, path);
        insertStmt.setLong(2, getCurrentRevision());
        insertStmt.setString(3, serializeProperties(properties));
        String parentPath = "";
        if (!path.equals("/")) {
            if (path.lastIndexOf("/") == 0) {
                parentPath = "/";
            } else {
                parentPath = path.substring(0, path.lastIndexOf("/"));
            }
        }
        insertStmt.setString(4, parentPath);

        insertStmt.execute();
    }

    String serializeProperties(Iterable<? extends PropertyState> properties) {
        return  gson.toJson(properties);
    }

    Map<String, PropertyState> deserializeProperties(String properties) {
        Type collectionType = new TypeToken<ArrayList<? extends PropertyState>>(){}.getType();
        System.out.println(properties);
        ArrayList<? extends PropertyState> list =  this.gson.fromJson(properties, collectionType);

        return Collections.emptyMap();
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

    @Override
    public Node getRootNode() {
        return getNode("/", getCurrentRevision());
    }

    @Override
    public TreeMap<String, Node> getNodeAndSubtree(String path, long revision, boolean wholeSubtree) {
        try (Connection connection = connectionPool.getConnection()) {
            TreeMap<String, Node> result = new TreeMap<>();

            if (wholeSubtree) {
                PreparedStatement preparedStatement = connection.prepareStatement(getSubtreeStmtString);
                preparedStatement.setString(1, path);
                preparedStatement.setString(2, path + "/%");
                preparedStatement.setString(3, path);
                preparedStatement.setString(4, path + "/%");
                preparedStatement.setLong(5, revision);

                ResultSet resultSet = preparedStatement.executeQuery();


                while (resultSet.next()) {
                    String nodePath = resultSet.getString(1);
                    Long nodeRevision = resultSet.getLong(2);
                    Map<String, PropertyState> nodeProperties = deserializeProperties(resultSet.getString(3));

                    Node node = new Node(nodePath.substring(nodePath.lastIndexOf('/') + 1), nodeProperties, nodeRevision);
                    result.put(nodePath, node);
                }
            } else {
                PreparedStatement preparedStatement = connection.prepareStatement(getChildNodesStmtString);

                preparedStatement.setString(1, path);
                preparedStatement.setString(2, path);
                preparedStatement.setString(3, path);
                preparedStatement.setString(4, path);
                preparedStatement.setLong(5, revision);

                ResultSet resultSet = preparedStatement.executeQuery();


                while (resultSet.next()) {
                    String nodePath = resultSet.getString(1);
                    Long nodeRevision = resultSet.getLong(2);
                    Map<String, PropertyState> nodeProperties = deserializeProperties(resultSet.getString(3));

                    Node node = new Node(nodePath.substring(nodePath.lastIndexOf('/') + 1), nodeProperties, nodeRevision);
                    result.put(nodePath, node);
                }
            }

            return result;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void moveChildNodes(String fromPath, String toPath) {

        TreeMap<String, Node> subtree = getNodeAndSubtree(fromPath, getCurrentRevision(), true);

        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement insertStmt = connection.prepareStatement(insertStmtString);
            connection.setAutoCommit(false);

            for (String nodePath : subtree.keySet()) {
                if (nodePath.equals(fromPath)) {
                    continue;
                }

                Node nodeToMove = subtree.get(nodePath);
                String newPath = nodePath.replace(fromPath, toPath);

                insertNewNode(newPath, nodeToMove.getProperties().values(), insertStmt);
            }

            connection.commit();
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public long incrementRevisionNumber() {
        return currentRevision.incrementAndGet();
    }

    @Override
    public long getCurrentRevision() {
        return currentRevision.get();
    }
}
