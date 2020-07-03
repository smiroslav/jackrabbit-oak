package org.apache.jackrabbit.oak.store.remote;

import org.apache.jackrabbit.oak.store.remote.store.MemoryStorage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RemoteNodeStoreTest {

    private RemoteNodeStore remoteNodeStore;
    private MemoryStorage storage;

    @Before
    public void setUp(){
        storage = new MemoryStorage();
    }

    @Test
    public void testGetRoot() {
        Assert.assertNull(storage.getRootNode());

        remoteNodeStore = new RemoteNodeStore(storage, null);

        Assert.assertNotNull(remoteNodeStore.getRoot());
        Assert.assertNotNull(storage.getRootNode());
    }
}
