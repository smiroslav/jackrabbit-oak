package org.apache.jackrabbit.oak.store.remote.store.cache;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.jackrabbit.oak.store.remote.store.ID;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.Store;
import org.apache.jackrabbit.oak.store.remote.store.Value;

public class CachedStore implements Store {

    private final Store store;

    private final Cache<ID, Node> nodeCache;

    private final Cache<ID, Map<String, Value>> propertiesCache;

    private final Cache<ID, Map<String, ID>> childrenCache;

    public CachedStore(
        Store store,
        Cache<ID, Node> nodeCache,
        Cache<ID, Map<String, Value>> propertiesCache,
        Cache<ID, Map<String, ID>> childrenCache
    ) {
        this.store = store;
        this.nodeCache = nodeCache;
        this.propertiesCache = propertiesCache;
        this.childrenCache = childrenCache;
    }

    @Override
    public ID getTag(String tag) throws IOException {
        return store.getTag(tag);
    }

    @Override
    public void putTag(String tag, ID id) throws IOException {
        store.putTag(tag, id);
    }

    @Override
    public void deleteTag(String tag) throws IOException {
        store.deleteTag(tag);
    }

    @Override
    public Node getNode(ID id) throws IOException {
        try {
            return nodeCache.get(id, () -> store.getNode(id));
        } catch (ExecutionException e) {
            throw (IOException) e.getCause();
        } catch (UncheckedExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @Override
    public ID putNode(ID properties, ID children) throws IOException {
        return store.putNode(properties, children);
    }

    @Override
    public Map<String, Value> getProperties(ID id) throws IOException {
        try {
            return propertiesCache.get(id, () -> store.getProperties(id));
        } catch (ExecutionException e) {
            throw (IOException) e.getCause();
        } catch (UncheckedExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @Override
    public ID putProperties(Map<String, Value> properties) throws IOException {
        return store.putProperties(properties);
    }

    @Override
    public Map<String, ID> getChildren(ID id) throws IOException {
        try {
            return childrenCache.get(id, () -> store.getChildren(id));
        } catch (ExecutionException e) {
            throw (IOException) e.getCause();
        } catch (UncheckedExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @Override
    public ID putChildren(Map<String, ID> children) throws IOException {
        return store.putChildren(children);
    }

}
