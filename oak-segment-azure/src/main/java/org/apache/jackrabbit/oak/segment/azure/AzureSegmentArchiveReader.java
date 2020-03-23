/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.azure;

import com.google.common.base.Stopwatch;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.azure.util.ExternalSegmentCache;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.Boolean.getBoolean;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.getSegmentFileName;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.readBufferFully;

public class AzureSegmentArchiveReader implements SegmentArchiveReader {
    static final boolean OFF_HEAP = getBoolean("access.off.heap");

    private static final String REDIS_PREFIX = "SEGMENT";

    private final CloudBlobDirectory archiveDirectory;

    private final IOMonitor ioMonitor;

    private final long length;

    private final Map<UUID, AzureSegmentArchiveEntry> index = new LinkedHashMap<>();
    private final ExternalSegmentCache externalSegmentCache;
    private  String tarCacheDirPath;

    private Boolean hasGraph;

    AzureSegmentArchiveReader(CloudBlobDirectory archiveDirectory, IOMonitor ioMonitor, ExternalSegmentCache externalSegmentCache) throws IOException {
        this.archiveDirectory = archiveDirectory;
        this.ioMonitor = ioMonitor;
        this.externalSegmentCache = externalSegmentCache;
        long length = 0;
        for (CloudBlob blob : AzureUtilities.getBlobs(archiveDirectory)) {
            Map<String, String> metadata = blob.getMetadata();
            if (AzureBlobMetadata.isSegment(metadata)) {
                AzureSegmentArchiveEntry indexEntry = AzureBlobMetadata.toIndexEntry(metadata, (int) blob.getProperties().getLength());
                index.put(new UUID(indexEntry.getMsb(), indexEntry.getLsb()), indexEntry);
            }
            length += blob.getProperties().getLength();
        }
        this.length = length;

        if (this.externalSegmentCache.isFileSystemCacheEnabled()) {
            tarCacheDirPath = externalSegmentCache.getFileSystemCacheLocation() + File.separator + archiveDirectory.getPrefix();
            File tarCacheDir = new File(tarCacheDirPath);
            if (!tarCacheDir.exists()) {
                tarCacheDir.mkdirs();
            }
        }

        this.externalSegmentCache.archiveReaderOpened();
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        AzureSegmentArchiveEntry indexEntry = index.get(new UUID(msb, lsb));
        if (indexEntry == null) {
            return null;
        }

        Buffer buffer;
        if (OFF_HEAP) {
            buffer = Buffer.allocateDirect(indexEntry.getLength());
        } else {
            buffer = Buffer.allocate(indexEntry.getLength());
        }
        ioMonitor.beforeSegmentRead(pathAsFile(), msb, lsb, indexEntry.getLength());
        Stopwatch stopwatch = Stopwatch.createStarted();

        String segmentFileName = getSegmentFileName(indexEntry);
        externalSegmentCache.loadSegment(tarCacheDirPath, segmentFileName, buffer, bufferParam -> readBufferFully(getBlob(segmentFileName), bufferParam));

        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        ioMonitor.afterSegmentRead(pathAsFile(), msb, lsb, indexEntry.getLength(), elapsed);
        return buffer;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return index.containsKey(new UUID(msb, lsb));
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        return new ArrayList<>(index.values());
    }

    @Override
    public Buffer getGraph() throws IOException {
        Buffer graph = readBlob(getName() + ".gph");
        hasGraph = graph != null;
        return graph;
    }

    @Override
    public boolean hasGraph() {
        if (hasGraph == null) {
            try {
                getGraph();
            } catch (IOException ignore) {
            }
        }
        return hasGraph;
    }

    @Override
    public Buffer getBinaryReferences() throws IOException {
        return readBlob(getName() + ".brf");
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public String getName() {
        return AzureUtilities.getName(archiveDirectory);
    }

    @Override
    public void close() {
        this.externalSegmentCache.archiveReaderClosed();
    }

    @Override
    public int getEntrySize(int size) {
        return size;
    }

    private File pathAsFile() {
        return new File(archiveDirectory.getUri().getPath());
    }

    private CloudBlockBlob getBlob(String name) throws IOException {
        try {
            return archiveDirectory.getBlockBlobReference(name);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    private Buffer readBlob(String name) throws IOException {
        try {
            CloudBlockBlob blob = getBlob(name);
            if (!blob.exists()) {
                return null;
            }
            long length = blob.getProperties().getLength();
            Buffer buffer = Buffer.allocate((int) length);
            AzureUtilities.readBufferFully(blob, buffer);
            return buffer;
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

}
