package org.apache.jackrabbit.oak.store.remote.store;

import java.sql.SQLException;

public class StorageException extends RuntimeException {

    public StorageException(SQLException e) {
        super(e);
    }
}
