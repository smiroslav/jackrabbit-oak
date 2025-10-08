package org.apache.jackrabbit.oak.security.authentication.sync;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class DefaultUserDiffPayloadQueue implements UserDiffPayloadQueue, Closeable {
    
    private static final Logger LOG = LoggerFactory.getLogger(DefaultUserDiffPayloadQueue.class);
    
    private final List<QueueEntry> payloadItems;
    private final ScheduledExecutorService executor;
    private final String instanceId;
    private int offset = 0;
    private Optional<Consumer<String>> userSyncConsumer = Optional.empty();

    public DefaultUserDiffPayloadQueue(String instanceId, List<QueueEntry> payloadItems) {
        this.instanceId = instanceId;
        this.payloadItems = payloadItems;
        this.executor = Executors.newSingleThreadScheduledExecutor();
        startProcessing();
    }

    private void startProcessing() {
        executor.scheduleAtFixedRate(() -> {
            try {
                if (offset < payloadItems.size()) {
                    QueueEntry entry = payloadItems.get(offset);
                    if (!entry.getInitiatorId().equals(instanceId)) {
                        LOG.debug("Processing change from initiator {} for consumer {}", 
                                entry.getInitiatorId(), instanceId);
                        userSyncConsumer.ifPresent(consumer -> consumer.accept(entry.getPayload()));
                    } else {
                        LOG.debug("Skipping change from same initiator {}", entry.getInitiatorId());
                    }
                    offset++;
                }
            } catch (Exception e) {
                LOG.error("Error processing payload item", e);
            }
        }, 0, 50, TimeUnit.MILLISECONDS);
    }

    @Override
    public void enqueue(String payload, String initiatorId) {
        LOG.debug("Enqueuing change from initiator {}", initiatorId);
        payloadItems.add(new QueueEntry(payload, initiatorId));
    }

    @Override
    public void registerQueueListener(Consumer<String> listener) {
        this.userSyncConsumer = Optional.of(listener);
    }

    @Override
    public void close() throws IOException {
        new ExecutorCloser(executor).close();
    }
}
