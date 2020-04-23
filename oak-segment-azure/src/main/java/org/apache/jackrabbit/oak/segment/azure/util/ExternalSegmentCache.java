package org.apache.jackrabbit.oak.segment.azure.util;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.azure.AzureUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Spliterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class ExternalSegmentCache {

    private static final Logger logger = LoggerFactory.getLogger(ExternalSegmentCache.class);

    private static final String REDIS_PREFIX = "SEGMENT";
    public static final int THREADS = Integer.getInteger("oak.segment.cache.threads", 10);

    private final boolean useFileSystemCache;

    private final String fileSystemCacheLocation;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final long cacheMaxSize;
    private final boolean useRedisCache;
    private final String redisHost;
    private final int redisExpireSeconds;

    private CachedSegmentRetriever fsSegments;
    private CachedSegmentRetriever redisSegments;
    private JedisPool redisPool;

    private AtomicLong cacheSize = new AtomicLong(0);
    private AtomicBoolean fsCacheCleanupInPrgress = new AtomicBoolean(false);

    private AtomicLong numberOfArchives = new AtomicLong(0);

    private  ExecutorService executor;

    public ExternalSegmentCache(boolean useFileSystemCache, String fileSystemCacheLocation, long fileSystemCacheMaxSize,
                                boolean useRedisCache, String redisHost, int redisExpireSeconds) {
        this.useFileSystemCache = useFileSystemCache;
        this.fileSystemCacheLocation = fileSystemCacheLocation;
        this.cacheMaxSize = fileSystemCacheMaxSize * 1024 * 1024;
        this.useRedisCache = useRedisCache;
        this.redisHost = redisHost;
        this.redisExpireSeconds = redisExpireSeconds;

        File fsCacheDir = new File(fileSystemCacheLocation);

        if (!fsCacheDir.exists()) {
            fsCacheDir.mkdirs();
        }

        cacheSize.set(FileUtils.sizeOfDirectory(fsCacheDir));

        initSegmentRetrievers();

        if (useFileSystemCache || useRedisCache) {
            executor = Executors.newFixedThreadPool(THREADS);
        }
    }

    private interface CachedSegmentRetriever {

        /**
         * Method returns true if segment is found in cache and loaded in Buffer
         */
        boolean loadSgment(String segmentPath, Buffer buffer) throws IOException;

        /**
         * Method that updates the cache with the content of the buffer
         */
        void updateCache(String segmentPath, Buffer buffer) throws IOException;
    }

    /**
     * Segment loader used if segment is not found in cache
     */
    public interface ExternalSegmentLoader{
        void loadSegment(Buffer buffer) throws IOException;
    }

    private void initSegmentRetrievers() {
        fsSegments = new CachedSegmentRetriever() {
            @Override
            public boolean loadSgment(String segmentPath, Buffer buffer) throws IOException {
                if (useFileSystemCache) {

                    File segmentFile = new File(segmentPath);

                    logger.trace("segmentPath = {} exists={}",segmentPath, segmentFile.exists());
                    if (segmentFile.exists()) {
                        try {
                            AzureUtilities.readBufferFullyFromFile(segmentFile, buffer);
                            return true;
                        } catch (FileNotFoundException e) {
                            logger.info("Segment deleted form file system: {}", segmentPath);
                        }
                    }
                }
                return false;
            }


            @Override
            public void updateCache(String segmentPath, Buffer buffer){
                if (useFileSystemCache) {
                    Buffer bufferCopy =  buffer.duplicate();

                    Runnable task = () -> {
                            try(FileChannel channel = new FileOutputStream(segmentPath).getChannel()) {
                                int fileSize = bufferCopy.write(channel);
                                cacheSize.addAndGet(fileSize);
                            } catch (FileNotFoundException e) {
                                logger.error("Error creating new file in segment cache: {}", segmentPath);
                            } catch (IOException e) {
                                logger.error("Error creating new file in segment cache: {}", segmentPath);
                            }

                            if (isCacheFull() && !fsCacheCleanupInPrgress.getAndSet(true)) {
                                cleanUpCache();
                                fsCacheCleanupInPrgress.set(false);
                            }
                    };

                    executor.execute(task);
                }
            }
        };

        if (useRedisCache) {
            int redisPort = 6379;
            int redisTimeout = 50;
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setTestOnBorrow(true);
            jedisPoolConfig.setMaxWaitMillis(50);
            jedisPoolConfig.setMaxIdle(20);
            jedisPoolConfig.setMaxTotal(200);
            this.redisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, redisTimeout);
        }

        redisSegments = new CachedSegmentRetriever() {
            @Override
            public boolean loadSgment(String segmentFileName, Buffer buffer) {
                if (useRedisCache) {
                    try(Jedis redis = redisPool.getResource()) {
                        final byte[] bytes = redis.get((REDIS_PREFIX + ":" + segmentFileName).getBytes());

                        if (bytes != null) {
                            buffer.put(bytes);
                            buffer.flip();
                            return true;
                        }
                    }
                }
                return false;
            }

            @Override
            public void updateCache(String segmentFileName, Buffer buffer) throws IOException {
                if (useRedisCache) {
                    Buffer bufferCopy = buffer.duplicate();

                    Runnable task = () -> {
                        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        try (WritableByteChannel channel = Channels.newChannel(bos); Jedis redis = redisPool.getResource()) {
                            while (bufferCopy.hasRemaining()) {
                                bufferCopy.write(channel);
                            }
                            redis.set((REDIS_PREFIX + ":" + segmentFileName).getBytes(), bos.toByteArray());
                            redis.expire((REDIS_PREFIX + ":" + segmentFileName).getBytes(), redisExpireSeconds);
                        } catch (IOException e) {
                            logger.error("Error updating redis cache with entry  for {}", segmentFileName);
                        }
                    };

                    executor.execute(task);
                }
            }
        };
    }

    public void loadSegment(String tarDirectoryPath, String segmentName, Buffer buffer, ExternalSegmentLoader externalSegmentLoader) throws IOException {
        String fsCacheSegmentPath = tarDirectoryPath + File.separator + segmentName;
        if (!fsSegments.loadSgment(fsCacheSegmentPath, buffer)) {
            if(!redisSegments.loadSgment(segmentName, buffer)) {
                externalSegmentLoader.loadSegment(buffer);
                redisSegments.updateCache(segmentName, buffer);
            }
            fsSegments.updateCache(fsCacheSegmentPath, buffer);
        }
    }


    /**
     *  Method that evicts least recently used segments form file system cache
     */
    public void cleanUpCache () {
        if (isCacheFull()) {

            File cacheDir = new File(fileSystemCacheLocation);

            try {
                Stream<Path> segmentsPaths = Files.walk(cacheDir.toPath())
                        .sorted((path1, path2) -> {
                            try {
                                FileTime lastAccessFile1 = Files.readAttributes(path1, BasicFileAttributes.class).lastAccessTime();
                                FileTime lastAccessFile2 = Files.readAttributes(path2, BasicFileAttributes.class).lastAccessTime();
                                return lastAccessFile1.compareTo(lastAccessFile2);
                            } catch (IOException e) {
                                //TODO
                            }
                            return 0;
                        })
                        .filter(filePath -> !filePath.toFile().isDirectory());

                StreamConsumer.forEach(segmentsPaths, (path, breaker) -> {

                    if (cacheSize.get() > cacheMaxSize * 0.66) {
                        cacheSize.addAndGet(-path.toFile().length());
                        path.toFile().delete();
                    } else {
                        breaker.stop();
                    }
                });
            } catch (IOException e) {
                //TODO
                e.printStackTrace();
            }
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

    private boolean isCacheFull() {
        return cacheSize.get() >= this.cacheMaxSize;
    }

    public void archiveReaderOpened() {
        numberOfArchives.incrementAndGet();
    }

    public void archiveReaderClosed() {
        /*
        long numOfOpenArchives = numberOfArchives.decrementAndGet();

        if (numOfOpenArchives == 0) {
            executor.shutdown();

            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                        System.err.println("Segment cache thread pool did not terminate");
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        */

    }
}

class StreamConsumer {

    public static class Breaker {
        private boolean shouldBreak = false;

        public void stop() {
            shouldBreak = true;
        }

        boolean get() {
            return shouldBreak;
        }
    }

    public static <T> void forEach(Stream<T> stream, BiConsumer<T, Breaker> consumer) {
        Spliterator<T> spliterator = stream.spliterator();
        boolean hadNext = true;
        Breaker breaker = new Breaker();

        while (hadNext && !breaker.get()) {
            hadNext = spliterator.tryAdvance(elem -> {
                consumer.accept(elem, breaker);
            });
        }
    }
}
