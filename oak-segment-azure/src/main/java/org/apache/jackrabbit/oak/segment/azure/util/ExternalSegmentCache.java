package org.apache.jackrabbit.oak.segment.azure.util;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ExternalSegmentCache {

    private final boolean useFileSystemCache;

    private final String fileSystemCacheLocation;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final long cacheMaxSize;
    private final boolean useRedisCache;

    private AtomicLong cacheSize = new AtomicLong(0);

    public ExternalSegmentCache(boolean useFileSystemCache, String fileSystemCacheLocation, int fileSystemCacheMaxSize,
                                boolean useRedisCache) {
        this.useFileSystemCache = useFileSystemCache;
        this.fileSystemCacheLocation = fileSystemCacheLocation;
        this.cacheMaxSize = fileSystemCacheMaxSize * 1024 * 1024;
        this.useRedisCache = useRedisCache;

        File fsCacheDir = new File(fileSystemCacheLocation);

        if (!fsCacheDir.exists()) {
            fsCacheDir.mkdirs();
        }
    }

    public boolean isFileSystemCacheEnabled() {
        return useFileSystemCache;
    }

    public boolean isRedisCacheEnabled() {
        return useRedisCache;
    }

    public String getFileSystemCacheLocation() {
        return fileSystemCacheLocation;
    }

    public AtomicLong cacheSize() {
        return cacheSize;
    }

    private boolean isCacheFull() {
        return cacheSize.get() >= this.cacheMaxSize;
    }

    /**
     *  Method that evicts least recently used segments form file system cache
     */
    public void cleanUpCache () {
        if (isCacheFull()) {

            File cacheDir = new File(fileSystemCacheLocation);

            File[] segments = cacheDir.listFiles();

            Arrays.sort(segments, (file1, file2) -> file1.lastModified() >= file2.lastModified() ? 1 : -1);

            for (File segment : segments) {
                if (cacheSize.get() > cacheMaxSize * 0.66) {
                    segment.delete();
                    cacheSize.addAndGet(-segment.length());
                }
            }
        }
    }
}
