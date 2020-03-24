package org.apache.jackrabbit.oak.segment.azure.util;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExternalSegmentCacheTest {

    private ExternalSegmentCache externalSegmentCache;
    private File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    private File cacheDir;

    @Before
    public void setUp() throws IOException {
        cacheDir = new File(tmpDir, "cache");
        cacheDir.mkdir();
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(cacheDir);
    }

    @Test
    public void testFsCacheCleanup() throws IOException {

        File data1 = new File(cacheDir, "data1");
        data1.mkdir();
        File data2 = new File(cacheDir, "data2");
        data2.mkdir();

        long segmentSize = 256l * 1024;

        File segment11 = new File(data1, "segment11");
        RandomAccessFile raFile11 = new RandomAccessFile(segment11, "rw");
        raFile11.setLength(segmentSize);

        File segment12 = new File(data1, "segment12");
        RandomAccessFile raFile12 = new RandomAccessFile(segment12, "rw");
        raFile12.setLength(segmentSize);

        File segment13 = new File(data1, "segment13");
        RandomAccessFile raFile13 = new RandomAccessFile(segment13, "rw");
        raFile13.setLength(segmentSize);


        File segment21 = new File(data2, "segment21");
        RandomAccessFile raFile21 = new RandomAccessFile(segment21, "rw");
        raFile21.setLength(segmentSize);

        File segment22 = new File(data2, "segment22");
        RandomAccessFile raFile22 = new RandomAccessFile(segment22, "rw");
        raFile22.setLength(segmentSize);


        File segment23 = new File(data2, "segment23");
        RandomAccessFile raFile23 = new RandomAccessFile(segment23, "rw");
        raFile23.setLength(segmentSize);

        externalSegmentCache = new ExternalSegmentCache(true, cacheDir.getAbsolutePath(), 1, false, "", 0);

        readSegment(segment11.getAbsolutePath(), (int)segmentSize);
        readSegment(segment21.getAbsolutePath(), (int)segmentSize);
        readSegment(segment13.getAbsolutePath(), (int)segmentSize);
        readSegment(segment23.getAbsolutePath(), (int)segmentSize);
        readSegment(segment12.getAbsolutePath(), (int)segmentSize);
        readSegment(segment22.getAbsolutePath(), (int)segmentSize);

        externalSegmentCache.cleanUpCache();

        assertTrue(segment22.exists());
        assertTrue(segment12.exists());

        assertFalse(segment23.exists());
        assertFalse(segment13.exists());
        assertFalse(segment21.exists());
        assertFalse(segment11.exists());

        System.out.println("file.exists()");
    }

    private void readSegment(String segmentPath, int bufferSize) throws IOException {
        Buffer buffer = Buffer.allocateDirect(bufferSize);
        try(FileChannel channel = new FileOutputStream(segmentPath).getChannel()) {
            buffer.write(channel);
        }
    }
}
