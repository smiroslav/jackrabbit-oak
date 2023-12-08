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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlobType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.microsoft.azure.storage.StorageException;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.jackrabbit.guava.common.collect.Lists.partition;

public class AzureJournalFile implements JournalFile {

    private static final Logger log = LoggerFactory.getLogger(AzureJournalFile.class);

    private static final int JOURNAL_LINE_LIMIT = Integer.getInteger("org.apache.jackrabbit.oak.segment.azure.journal.lines", 40_000);

    private final String journalNamePrefix;

    private final int lineLimit;

    private final WriteAccessController writeAccessController;

    private BlobContainerClient blobContainerClient;

    AzureJournalFile(BlobContainerClient blobContainerClient, String journalNamePrefix, WriteAccessController writeAccessController, int lineLimit) {
        this.blobContainerClient = blobContainerClient;
        this.journalNamePrefix = journalNamePrefix;
        this.lineLimit = lineLimit;
        this.writeAccessController = writeAccessController;
    }

    public AzureJournalFile(BlobContainerClient blobContainerClient, String journalNamePrefix, WriteAccessController writeAccessController) {
        this(blobContainerClient, journalNamePrefix, writeAccessController, JOURNAL_LINE_LIMIT);
    }

    @Override
    public JournalFileReader openJournalReader() throws IOException {
        return new CombinedReader(getJournalAppendBlobs());
    }

    @Override
    public JournalFileWriter openJournalWriter() throws IOException {
        return new AzureJournalWriter();
    }

    @Override
    public String getName() {
        return journalNamePrefix;
    }

    @Override
    public boolean exists() {
        try {
            return !getJournalAppendBlobs().isEmpty();
        } catch (IOException e) {
            log.error("Can't check if the file exists", e);
            return false;
        }
    }

    private String getJournalFileName(int index) {
        return String.format("%s.%03d", journalNamePrefix, index);
    }

    private List<AppendBlobClient> getJournalAppendBlobs() throws IOException {

            List<AppendBlobClient> result = new ArrayList<>();
            ListBlobsOptions options = new ListBlobsOptions();
            options.setPrefix(journalNamePrefix);
            blobContainerClient.listBlobs(options, null)
                    .stream()
                    .filter(blobItem -> blobItem.getProperties().getBlobType().equals(BlobType.APPEND_BLOB))
                    .forEach(blobItem -> result.add(blobContainerClient.getBlobClient(blobItem.getName()).getAppendBlobClient()));


            result.sort(Comparator.<AppendBlobClient, String>comparing(appendBlobClient -> AzureUtilities.getName(appendBlobClient.getBlobName())).reversed());
            return result;
    }

    private static class AzureJournalReader implements JournalFileReader {

        private AppendBlobClient appendBlobClient;

        private ReverseFileReader reader;

        private boolean metadataFetched;

        private boolean firstLineReturned;

        private AzureJournalReader(AppendBlobClient appendBlobClient) {
            this.appendBlobClient = appendBlobClient;
        }

        @Override
        public String readLine() throws IOException {
            if (reader == null) {
                try {
                    if (!metadataFetched) {
                        Map<String, String> metadata = appendBlobClient.getProperties().getMetadata();
                        metadataFetched = true;
                        if (metadata.containsKey("lastEntry")) {
                            firstLineReturned = true;
                            return metadata.get("lastEntry");
                        }
                    }
                    reader = new ReverseFileReader(appendBlobClient);
                    if (firstLineReturned) {
                        while("".equals(reader.readLine())); // the first line was already returned, let's fast-forward it
                    }
                } catch (StorageException e) {
                    throw new IOException(e);
                }
            }
            return reader.readLine();
        }

        @Override
        public void close() throws IOException {
        }
    }

    private class AzureJournalWriter implements JournalFileWriter {
        private AppendBlobClient appendBlobClient;

        private int lineCount;

        public AzureJournalWriter() throws IOException {
            List<AppendBlobClient> appendBlobClients = getJournalAppendBlobs();
            if (appendBlobClients.isEmpty()) {
                try {
                    appendBlobClient = blobContainerClient.getBlobClient(getJournalFileName(1)).getAppendBlobClient();
                    appendBlobClient.createIfNotExists();
                } catch (BlobStorageException e) {
                    throw new IOException(e);
                }
            } else {
                appendBlobClient = appendBlobClients.get(0);
            }
            String lc = appendBlobClient.getProperties().getMetadata().get("lineCount");
            lineCount = lc == null ? 0 : Integer.parseInt(lc);
        }

        @Override
        public void truncate() throws IOException {
            try {
                writeAccessController.checkWritingAllowed();

                for (AppendBlobClient appendBlobClient : getJournalAppendBlobs()) {
                    appendBlobClient.delete();
                }

                createNextFile(0);
            } catch (BlobStorageException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void writeLine(String line) throws IOException {
            batchWriteLines(ImmutableList.of(line));
        }

        @Override
        public void batchWriteLines(List<String> lines) throws IOException {
            writeAccessController.checkWritingAllowed();

            if (lines.isEmpty()) {
                return;
            }
            int firstBlockSize = Math.min(lineLimit - lineCount, lines.size());
            List<String> firstBlock = lines.subList(0, firstBlockSize);
            List<List<String>> remainingBlocks = partition(lines.subList(firstBlockSize, lines.size()), lineLimit);
            List<List<String>> allBlocks = ImmutableList.<List<String>>builder()
                .addAll(firstBlock.isEmpty() ? ImmutableList.of() : ImmutableList.of(firstBlock))
                .addAll(remainingBlocks)
                .build();

            for (List<String> entries : allBlocks) {
                if (lineCount >= lineLimit) {
                    int parsedSuffix = parseCurrentSuffix();
                    createNextFile(parsedSuffix);
                }
                StringBuilder text = new StringBuilder();
                for (String line : entries) {
                    text.append(line).append("\n");
                }
                try {
                    appendBlobClient.appendBlock(new ByteArrayInputStream(text.toString().getBytes()), text.length());
                    Map<String, String> metadata = appendBlobClient.getProperties().getMetadata();
                    metadata.put("lastEntry", entries.get(entries.size() - 1));
                    lineCount += entries.size();
                    metadata.put("lineCount", Integer.toString(lineCount));
                    appendBlobClient.setMetadata(metadata);
                } catch (BlobStorageException e) {
                    throw new IOException(e);
                }
            }
        }

        private void createNextFile(int suffix) throws IOException {
            try {
                appendBlobClient = blobContainerClient.getBlobClient(getJournalFileName(suffix + 1)).getAppendBlobClient();
                appendBlobClient.create(true);
                lineCount = 0;
            } catch (BlobStorageException e) {
                throw new IOException(e);
            }
        }

        private int parseCurrentSuffix() {
            String name = appendBlobClient.getBlobName();
            Pattern pattern = Pattern.compile(Pattern.quote(journalNamePrefix) + "\\.(\\d+)" );
            Matcher matcher = pattern.matcher(name);
            int parsedSuffix;
            if (matcher.find()) {
                String suffix = matcher.group(1);
                try {
                    parsedSuffix = Integer.parseInt(suffix);
                } catch (NumberFormatException e) {
                    log.warn("Can't parse suffix for journal file {}", name);
                    parsedSuffix = 0;
                }
            } else {
                log.warn("Can't parse journal file name {}", name);
                parsedSuffix = 0;
            }
            return parsedSuffix;
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    }

    private static class CombinedReader implements JournalFileReader {

        private final Iterator<AzureJournalReader> readers;

        private JournalFileReader currentReader;

        private CombinedReader(List<AppendBlobClient> appendBlobClients) {
            readers = appendBlobClients.stream().map(AzureJournalReader::new).iterator();
        }

        @Override
        public String readLine() throws IOException {
            String line;
            do {
                if (currentReader == null) {
                    if (!readers.hasNext()) {
                        return null;
                    }
                    currentReader = readers.next();
                }
                do {
                    line = currentReader.readLine();
                } while ("".equals(line));
                if (line == null) {
                    currentReader.close();
                    currentReader = null;
                }
            } while (line == null);
            return line;
        }

        @Override
        public void close() throws IOException {
            while (readers.hasNext()) {
                readers.next().close();
            }
            if (currentReader != null) {
                currentReader.close();
                currentReader = null;
            }
        }
    }
}
