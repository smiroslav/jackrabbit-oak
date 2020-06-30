package org.apache.jackrabbit.oak.kv.store.metrics;

import static com.google.common.base.Stopwatch.createStarted;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Store;
import org.apache.jackrabbit.oak.kv.store.Value;

public class MetricsStore implements Store {

    private final Store store;

    private final Metrics metrics;

    public MetricsStore(Store store, Metrics metrics) {
        this.store = store;
        this.metrics = metrics;
    }

    @Override
    public ID getTag(String tag) throws IOException {
        Stopwatch stopwatch = createStarted();
        ID result = store.getTag(tag);
        metrics.onTagGet(stopwatch.elapsed(NANOSECONDS));
        return result;
    }

    @Override
    public void putTag(String tag, ID id) throws IOException {
        Stopwatch stopwatch = createStarted();
        store.putTag(tag, id);
        metrics.onTagPut(stopwatch.elapsed(NANOSECONDS));
    }

    @Override
    public void deleteTag(String tag) throws IOException {
        Stopwatch stopwatch = createStarted();
        store.deleteTag(tag);
        metrics.onTagDelete(stopwatch.elapsed(NANOSECONDS));
    }

    @Override
    public Node getNode(ID id) throws IOException {
        Stopwatch stopwatch = createStarted();
        Node node = store.getNode(id);
        metrics.onNodeGet(stopwatch.elapsed(NANOSECONDS));
        return node;
    }

    @Override
    public ID putNode(ID properties, ID children) throws IOException {
        Stopwatch stopwatch = createStarted();
        ID id = store.putNode(properties, children);
        metrics.onNodePut(stopwatch.elapsed(NANOSECONDS));
        return id;
    }

    @Override
    public Map<String, Value> getProperties(ID id) throws IOException {
        Stopwatch stopwatch = createStarted();
        Map<String, Value> properties = store.getProperties(id);
        metrics.onPropertiesGet(stopwatch.elapsed(NANOSECONDS));
        return properties;
    }

    @Override
    public ID putProperties(Map<String, Value> properties) throws IOException {
        Stopwatch stopwatch = createStarted();
        ID id = store.putProperties(properties);
        metrics.onPropertiesPut(stopwatch.elapsed(NANOSECONDS));
        return id;
    }

    @Override
    public Map<String, ID> getChildren(ID id) throws IOException {
        Stopwatch stopwatch = createStarted();
        Map<String, ID> children = store.getChildren(id);
        metrics.onChildrenGet(stopwatch.elapsed(NANOSECONDS));
        return children;
    }

    @Override
    public ID putChildren(Map<String, ID> children) throws IOException {
        Stopwatch stopwatch = createStarted();
        ID id = store.putChildren(children);
        metrics.onChildrenPut(stopwatch.elapsed(NANOSECONDS));
        return id;
    }

}
