package org.apache.jackrabbit.oak.store.remote.store;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;

public class MemoryStorageTest {

    MemoryStorage memoryStorage;

    @Before
    public void setUp() {
        memoryStorage = new MemoryStorage();
    }

    @Test
    public void testStorage() {
        memoryStorage.addNode("/", Collections.emptyMap(), 1);
        memoryStorage.addNode("/a", Collections.emptyMap(), 1);
        memoryStorage.addNode("/b", Collections.emptyMap(), 1);
        memoryStorage.addNode("/b/d", Collections.emptyMap(), 1);
        memoryStorage.addNode("/c", Collections.emptyMap(), 1);
        memoryStorage.addNode("/c/e", Collections.emptyMap(), 1);

        Map<String, MemoryStorage.Node> result = memoryStorage.getNodeAndSubtree("/", 1, false);
        assertEquals(4, result.size());

        result = memoryStorage.getNodeAndSubtree("/", 1, true);
        assertEquals(6, result.size());
    }
}
