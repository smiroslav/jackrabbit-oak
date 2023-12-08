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
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RequestCompletedEvent;
import com.microsoft.azure.storage.RetryLinearRetry;
import com.microsoft.azure.storage.StorageEvent;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class AzurePersistence implements SegmentNodeStorePersistence {

    private static final String RETRY_ATTEMPTS_PROP = "segment.azure.retry.attempts";
    private static final int DEFAULT_RETRY_ATTEMPTS = 5;

    private static final String RETRY_BACKOFF_PROP = "segment.azure.retry.backoff";
    private static final int DEFAULT_RETRY_BACKOFF_SECONDS = 5;

    private static final String TIMEOUT_EXECUTION_PROP = "segment.timeout.execution";
    private static final int DEFAULT_TIMEOUT_EXECUTION = 30;

    private static final String TIMEOUT_INTERVAL_PROP = "segment.timeout.interval";
    private static final int DEFAULT_TIMEOUT_INTERVAL = 1;

    private static final Logger log = LoggerFactory.getLogger(AzurePersistence.class);
    protected String rootPrefix;

    protected BlobContainerClient blobContainerClient;

    protected CloudBlobDirectory segmentstoreDirectory;

    protected WriteAccessController writeAccessController = new WriteAccessController();

    public AzurePersistence(CloudBlobDirectory segmentStoreDirectory) {
        this.segmentstoreDirectory = segmentStoreDirectory;

        BlobRequestOptions defaultRequestOptions = segmentStoreDirectory.getServiceClient().getDefaultRequestOptions();
        if (defaultRequestOptions.getRetryPolicyFactory() == null) {
            int retryAttempts = Integer.getInteger(RETRY_ATTEMPTS_PROP, DEFAULT_RETRY_ATTEMPTS);
            if (retryAttempts > 0) {
                Integer retryBackoffSeconds = Integer.getInteger(RETRY_BACKOFF_PROP, DEFAULT_RETRY_BACKOFF_SECONDS);
                defaultRequestOptions.setRetryPolicyFactory(new RetryLinearRetry((int) TimeUnit.SECONDS.toMillis(retryBackoffSeconds), retryAttempts));
            }
        }
        if (defaultRequestOptions.getMaximumExecutionTimeInMs() == null) {
            int timeoutExecution = Integer.getInteger(TIMEOUT_EXECUTION_PROP, DEFAULT_TIMEOUT_EXECUTION);
            if (timeoutExecution > 0) {
                defaultRequestOptions.setMaximumExecutionTimeInMs((int) TimeUnit.SECONDS.toMillis(timeoutExecution));
            }
        }
        if (defaultRequestOptions.getTimeoutIntervalInMs() == null) {
            int timeoutInterval = Integer.getInteger(TIMEOUT_INTERVAL_PROP, DEFAULT_TIMEOUT_INTERVAL);
            if (timeoutInterval > 0) {
                defaultRequestOptions.setTimeoutIntervalInMs((int) TimeUnit.SECONDS.toMillis(timeoutInterval));
            }
        }
    }

    public AzurePersistence(BlobContainerClient blobContainerClient, String rootPrefix) {
        this.blobContainerClient = blobContainerClient;

        this.rootPrefix = rootPrefix;
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
        attachRemoteStoreMonitor(remoteStoreMonitor);
        return new AzureArchiveManager(blobContainerClient, rootPrefix, ioMonitor, fileStoreMonitor, writeAccessController);
    }

    @Override
    public boolean segmentFilesExist() {

        return blobContainerClient.listBlobsByHierarchy(rootPrefix + "/").stream()
                .filter(blobItem -> blobItem.isPrefix())
                .filter(blobItem -> blobItem.getName().endsWith(".tar") || blobItem.getName().endsWith(".tar/"))
                .findFirst().isPresent();
    }

    @Override
    public JournalFile getJournalFile() {
        return new AzureJournalFile(blobContainerClient, rootPrefix + "/journal.log", writeAccessController);
    }

    @Override
    public GCJournalFile getGCJournalFile() throws IOException {
        return new AzureGCJournalFile(blobContainerClient.getBlobClient(rootPrefix + "/gc.log").getAppendBlobClient());
    }

    @Override
    public ManifestFile getManifestFile() {
        return new AzureManifestFile(blobContainerClient.getBlobClient(rootPrefix + "/manifest").getBlockBlobClient());
    }

    @Override
    public RepositoryLock lockRepository() throws IOException {
        BlockBlobClient blobClient = blobContainerClient.getBlobClient(rootPrefix + "/repo.lock").getBlockBlobClient();
        BlobLeaseClient leaseClient = new BlobLeaseClientBuilder().blobClient(blobClient).buildClient();

        return new AzureRepositoryLock(blobClient, leaseClient, () -> {
            log.warn("Lost connection to the Azure. The client will be closed.");
            // TODO close the connection
        }, writeAccessController).lock();
    }

    private static void attachRemoteStoreMonitor(RemoteStoreMonitor remoteStoreMonitor) {
        OperationContext.getGlobalRequestCompletedEventHandler().addListener(new StorageEvent<RequestCompletedEvent>() {

            @Override
            public void eventOccurred(RequestCompletedEvent e) {
                Date startDate = e.getRequestResult().getStartDate();
                Date stopDate = e.getRequestResult().getStopDate();

                if (startDate != null && stopDate != null) {
                    long requestDuration = stopDate.getTime() - startDate.getTime();
                    remoteStoreMonitor.requestDuration(requestDuration, TimeUnit.MILLISECONDS);
                }

                Exception exception = e.getRequestResult().getException();

                if (exception == null) {
                    remoteStoreMonitor.requestCount();
                } else {
                    remoteStoreMonitor.requestError();
                }
            }

        });
    }

    public CloudBlobDirectory getSegmentstoreDirectory() {
        return segmentstoreDirectory;
    }

    public void setWriteAccessController(WriteAccessController writeAccessController) {
        this.writeAccessController = writeAccessController;
    }
}
