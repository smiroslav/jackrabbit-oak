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
import org.apache.jackrabbit.oak.segment.azure.AzureJournalFile;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.JournalReaderTest;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzureJournalReaderTest extends JournalReaderTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient blobContainerClient;

    private AppendBlobClient appendBlobClient;

    private String journalName;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        blobContainerClient = azurite.getBlobContainerClient("oak-test");
        journalName = "journal/journal.log.001";
        appendBlobClient = blobContainerClient.getBlobClient(journalName).getAppendBlobClient();
    }

    protected JournalReader createJournalReader(String s) throws IOException {
        try {
            appendBlobClient.create(true);

            if (s.length() > 0) {
                appendBlobClient.appendBlock(new ByteArrayInputStream(s.getBytes()), s.length());
            }

            return new JournalReader(new AzureJournalFile(blobContainerClient, journalName, new WriteAccessController()));
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }
}
