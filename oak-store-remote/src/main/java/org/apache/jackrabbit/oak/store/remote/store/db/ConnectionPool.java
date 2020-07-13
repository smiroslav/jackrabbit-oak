package org.apache.jackrabbit.oak.store.remote.store.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionPool {
    Connection getConnection() throws SQLException;
}
