/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.http.RequestConditions;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.microsoft.azure.storage.StorageException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;

public class AzureRepositoryLockTest {

    private static final Logger log = LoggerFactory.getLogger(AzureRepositoryLockTest.class);
    public static final String LEASE_DURATION = "15";
    public static final String RENEWAL_INTERVAL = "3";
    public static final String TIME_TO_WAIT_BEFORE_BLOCK = "9";

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient blobContainerClient;

    private BlockBlobClient blobClient;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        blobContainerClient = azurite.getBlobContainerClient("oak-test");

        blobClient = blobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
    }

    @Rule
    public final ProvideSystemProperty systemPropertyRule = new ProvideSystemProperty(AzureRepositoryLock.LEASE_DURATION_PROP, LEASE_DURATION)
            .and(AzureRepositoryLock.RENEWAL_INTERVAL_PROP, RENEWAL_INTERVAL)
            .and(AzureRepositoryLock.TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, TIME_TO_WAIT_BEFORE_BLOCK);

    @Test
    public void testFailingLock() throws IOException {
        new AzureRepositoryLock(blobClient, createLeaseClient(), () -> {}, new WriteAccessController()).lock();
        try {
            new AzureRepositoryLock(blobClient, createLeaseClient(), () -> {}, new WriteAccessController()).lock();
            fail("The second lock should fail.");
        } catch (IOException e) {
            // it's fine
        }
    }

    @Test
    public void testWaitingLock() throws IOException, InterruptedException {
        Semaphore s = new Semaphore(0);
        new Thread(() -> {
            try {
                RepositoryLock lock = new AzureRepositoryLock(blobClient, createLeaseClient(), () -> {}, new WriteAccessController());
                s.release();
                Thread.sleep(1000);
                lock.unlock();
            } catch (Exception e) {
                log.error("Can't lock or unlock the repo", e);
            }
        }).start();

        s.acquire();
        new AzureRepositoryLock(blobClient, createLeaseClient(), () -> {}, new WriteAccessController()).lock();
    }

    @Test
    public void testLeaseRefreshUnsuccessful() throws URISyntaxException, StorageException, IOException, InterruptedException {
        BlobLeaseClient leaseClientMocked = spy(createLeaseClient());

        BlockBlobClient blobClientMocked = spy(blobClient);

        // instrument the mock to throw the exception twice when renewing the lease
        BlobStorageException blobStorageException = spy(new BlobStorageException("operation timeout", null, null));
        Mockito.doReturn(BlobErrorCode.OPERATION_TIMED_OUT).when(blobStorageException).getErrorCode();


        Mockito.doThrow(blobStorageException)
                .doThrow(blobStorageException)
                .doCallRealMethod()
                .when(leaseClientMocked).renewLeaseWithResponse((RequestConditions) Mockito.any(), Mockito.any(), Mockito.any());

        // lock file not created yet
        Mockito.doReturn(false).when(blobClientMocked).exists();

        new AzureRepositoryLock(blobClientMocked, leaseClientMocked, () -> {}, new WriteAccessController()).lock();

        // wait till lease expires
        Thread.sleep(16000);

        // lock file exists
        Mockito.doReturn(true).when(blobClientMocked).exists();
        // reset default behaviour for lease renewal
        Mockito.doCallRealMethod().when(leaseClientMocked).renewLeaseWithResponse((RequestConditions) Mockito.any(), Mockito.any(), Mockito.any());

        try {
            new AzureRepositoryLock(blobClient, createLeaseClient(), () -> {}, new WriteAccessController()).lock();
            fail("The second lock should should succeed since previous one has expired.");
        } catch (IOException e) {
            // it's fine
        }
    }

    @Test
    public void testWritesBlockedOnlyAfterFewUnsuccessfulAttempts() throws Exception {
        BlobLeaseClient leaseClientMocked = spy(createLeaseClient());

        BlockBlobClient blobClientMocked = spy(blobClient);

        // instrument the mock to throw the exception twice when renewing the lease
        BlobStorageException blobStorageException = spy(new BlobStorageException("operation timeout", null, null));
        Mockito.doReturn(BlobErrorCode.OPERATION_TIMED_OUT).when(blobStorageException).getErrorCode();

        Mockito
                .doCallRealMethod()
                .doThrow(blobStorageException)
                .when(leaseClientMocked).renewLeaseWithResponse((RequestConditions) Mockito.any(), Mockito.any(), Mockito.any());


        WriteAccessController writeAccessController = new WriteAccessController();

        // lock file not created yet
        Mockito.doReturn(false).when(blobClientMocked).exists();

        new AzureRepositoryLock(blobClientMocked, leaseClientMocked, () -> {}, writeAccessController).lock();


        Thread thread = new Thread(() -> {

            while (true) {
                writeAccessController.checkWritingAllowed();

            }
        });

        thread.start();

        Thread.sleep(3000);
        assertFalse("after 3 seconds thread should not be in a waiting state", thread.getState().equals(Thread.State.WAITING));

        Thread.sleep(3000);
        assertFalse("after 6 seconds thread should not be in a waiting state", thread.getState().equals(Thread.State.WAITING));

        Thread.sleep(5000);
        assertTrue("after more than 9 seconds thread should be in a waiting state", thread.getState().equals(Thread.State.WAITING));

        Mockito.doCallRealMethod().when(leaseClientMocked).renewLeaseWithResponse((RequestConditions) Mockito.any(), Mockito.any(), Mockito.any());
    }

    private BlobLeaseClient createLeaseClient() {
        return new BlobLeaseClientBuilder()
                .blobClient(blobClient)
                .buildClient();
    }
}
