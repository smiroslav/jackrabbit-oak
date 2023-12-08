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
package org.apache.jackrabbit.oak.segment.azure.journal;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.microsoft.azure.storage.StorageException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.azure.ReverseFileReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ReverseFileReaderTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient blobContainerClient;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        blobContainerClient = azurite.getBlobContainerClient("oak-test");
    }

    private AppendBlobClient getAppendBlobClient() throws URISyntaxException, StorageException {
        AppendBlobClient appendBlobClient = blobContainerClient.getBlobClient("test-blob").getAppendBlobClient();
        appendBlobClient.createIfNotExists();
        return appendBlobClient;
    }

    @Test
    public void testReverseReader() throws IOException, URISyntaxException, StorageException {
        List<String> entries = createFile( 1024, 80);
        ReverseFileReader reader = new ReverseFileReader(getAppendBlobClient(), 256);
        assertEquals(entries, reader);
    }

    @Test
    public void testEmptyFile() throws IOException, URISyntaxException, StorageException {
        List<String> entries = createFile( 0, 80);
        ReverseFileReader reader = new ReverseFileReader(getAppendBlobClient(), 256);
        assertEquals(entries, reader);
    }

    @Test
    public void test1ByteBlock() throws IOException, URISyntaxException, StorageException {
        List<String> entries = createFile( 10, 16);
        ReverseFileReader reader = new ReverseFileReader(getAppendBlobClient(), 256);
        assertEquals(entries, reader);
    }

    private List<String> createFile(int lines, int maxLineLength) throws IOException, URISyntaxException, StorageException {
        Random random = new Random();
        List<String> entries = new ArrayList<>();
        AppendBlobClient appendBlobClient = getAppendBlobClient();
        for (int i = 0; i < lines; i++) {
            int entrySize = random.nextInt(maxLineLength) + 1;
            String entry = randomString(entrySize);
            try {

                appendBlobClient.appendBlock(new ByteArrayInputStream((entry + '\n').getBytes()), entry.length() + 1);
            } catch (BlobStorageException e) {
                throw new IOException(e);
            }
            entries.add(entry);
        }

        entries.add("");
        Collections.reverse(entries);
        return entries;
    }

    private static void assertEquals(List<String> entries, ReverseFileReader reader) throws IOException {
        int i = entries.size();
        for (String e : entries) {
            Assert.assertEquals("line " + (--i), e, reader.readLine());
        }
        Assert.assertNull(reader.readLine());
    }

    private static String randomString(int entrySize) {
        Random r = new Random();

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < entrySize; i++) {
            result.append((char) ('a' + r.nextInt('z' - 'a')));
        }

        return result.toString();
    }
}
