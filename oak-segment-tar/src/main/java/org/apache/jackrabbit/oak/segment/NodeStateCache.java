package org.apache.jackrabbit.oak.segment;

import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class NodeStateCache{

    private final Cache<String, SegmentNodeState> cache;

    public NodeStateCache(@NotNull Supplier<SegmentWriter> writer, @Nullable BlobStore blobStore, MeterStats readStats, SegmentReader segmentReader) {
        this.cache = CacheBuilder.newBuilder()
                .concurrencyLevel(16)
                .maximumSize(100000)
                .build();
    }

    public SegmentNodeState getNodeState(@NotNull RecordId id, @NotNull Callable<SegmentNodeState> loader) throws ExecutionException {

        //int cacheKey = getEntryHash(id.getSegmentId().getLeastSignificantBits(), id.getSegmentId().getMostSignificantBits(), id.getRecordNumber());
        String cacheKey = id.asUUID().toString() + id.getRecordNumber();
        return cache.get(cacheKey, () -> loader.call());
    }

    private static int getEntryHash(long lsb, long msb, int offset) {
        int hash = (int) (msb ^ lsb) + offset;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        return (hash >>> 16) ^ hash;
    }
}
