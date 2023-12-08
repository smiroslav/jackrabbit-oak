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

import com.azure.core.http.RequestConditions;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.microsoft.azure.storage.StorageException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.AbstractPersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.CachingPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.split.SplitPersistence;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.*;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;

public class AzureArchiveManagerTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private String rootPrefix = "oak";

    BlobContainerClient blobContainerClient;

    private AzurePersistence azurePersistence;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        blobContainerClient = azurite.getBlobContainerClient("oak-test");

        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();

        azurePersistence = new AzurePersistence(blobContainerClient, rootPrefix);
        azurePersistence.setWriteAccessController(writeAccessController);
    }

    @Rule
    public final ProvideSystemProperty systemPropertyRule = new ProvideSystemProperty(AzureRepositoryLock.LEASE_DURATION_PROP, "15")
            .and(AzureRepositoryLock.RENEWAL_INTERVAL_PROP, "3")
            .and(AzureRepositoryLock.TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, "9");

    @Test
    public void testRecovery() throws StorageException, URISyntaxException, IOException {
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix("oak/data00000a.tar/0005.");
        blobContainerClient.listBlobs(options, null).forEach(blobItem -> {
            blobContainerClient.getBlobClient(blobItem.getName()).getBlockBlobClient().delete();
        });

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries("data00000a.tar", recovered);
        assertEquals(uuids.subList(0, 5), newArrayList(recovered.keySet()));
    }

    @Test
    public void testBackupWithRecoveredEntries() throws StorageException, URISyntaxException, IOException {
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        blobContainerClient.getBlobClient("oak/data00000a.tar/0005." + uuids.get(5).toString()).getBlockBlobClient().delete();

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries("data00000a.tar", recovered);

        manager.backup("data00000a.tar", "data00000a.tar.bak", recovered.keySet());

        for (int i = 0; i <= 4; i++) {
            assertTrue(blobExists("oak/data00000a.tar/000"+ i + "." + uuids.get(i)));
        }

        for (int i = 5; i <= 9; i++) {
            assertFalse(String.format("Segment %s.??? should have been deleted.", "oak/data00000a.tar/000"+ i),
                    blobContainerClient.getBlobClient("oak/data00000a.tar/000"+ i + "." + uuids.get(i)).exists());
        }
    }

    @Test
    public void testUncleanStop() throws Exception {
        AzurePersistence p = new AzurePersistence(blobContainerClient, rootPrefix);
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        blobContainerClient.getBlobClient("oak/data00000a.tar/closed").getBlockBlobClient().delete();
        blobContainerClient.getBlobClient("oak/data00000a.tar/data00000a.tar.brf").getBlockBlobClient().delete();
        blobContainerClient.getBlobClient("oak/data00000a.tar/data00000a.tar.gph").getBlockBlobClient().delete();

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));
        fs.close();
    }

    @Test
    // see OAK-8566
    public void testUncleanStopWithEmptyArchive() throws URISyntaxException, IOException, InvalidFileStoreVersionException, CommitFailedException, StorageException {
        AzurePersistence p = new AzurePersistence(blobContainerClient, rootPrefix);
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        // make sure there are 2 archives
        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo2", "bar2");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        // remove the segment 0000 from the second archive
        deleteFirstWithPrefix("oak/data00001a.tar/0000.");
        deleteBlob("oak/data00001a.tar/closed");

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));
        fs.close();
    }

    @Test
    public void testUncleanStopSegmentMissing() throws URISyntaxException, IOException, InvalidFileStoreVersionException, CommitFailedException, StorageException {
        AzurePersistence p = new AzurePersistence(blobContainerClient, rootPrefix);
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        // make sure there are 2 archives
        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo0", "bar0");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.flush();
        //create segment 0001
        builder.setProperty("foo1", "bar1");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.flush();
        //create segment 0002
        builder.setProperty("foo2", "bar2");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.flush();
        //create segment 0003
        builder.setProperty("foo3", "bar3");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.flush();
        fs.close();

        // remove the segment 0002 from the second archive
        deleteFirstWithPrefix("oak/data00001a.tar/0002.");
        deleteBlob("oak/data00001a.tar/closed");

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));

        //recovered archive data00001a.tar should not contain segments 0002 and 0003
        assertFalse(blobExists("oak/data00001a.tar/0002."));
        assertFalse(blobExists("oak/data00001a.tar/0003."));

        assertTrue("Backup directory should have been created", blobExists("oak/data00001a.tar.bak"));
        //backup has all segments but 0002 since it was deleted before recovery

        assertTrue(blobExists("oak/data00001a.tar.bak/0001."));
        assertFalse(blobExists("oak/data00001a.tar.bak/0002."));
        assertTrue(blobExists("oak/data00001a.tar.bak/0003."));

        //verify content from recovered segments preserved
        assertEquals("bar1", segmentNodeStore.getRoot().getString("foo1"));
        //content from deleted segments not preserved
        assertNull(segmentNodeStore.getRoot().getString("foo2"));
        assertNull(segmentNodeStore.getRoot().getString("foo3"));
        fs.close();
    }

    @Test
    public void testArchiveExistsAfterFlush() throws URISyntaxException, IOException {
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        Assert.assertFalse(manager.exists("data00000a.tar"));
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
        Assert.assertTrue(manager.exists("data00000a.tar"));
    }

    @Test(expected = FileNotFoundException.class)
    public void testSegmentDeletedAfterCreatingReader() throws IOException, URISyntaxException, StorageException {
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        Assert.assertFalse(manager.exists("data00000a.tar"));
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
        writer.close();

        SegmentArchiveReader reader = manager.open("data00000a.tar");
        Buffer segment = reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits());
        assertNotNull(segment);

        deleteFirstWithPrefix("oak/data00000a.tar/0000.");

        try {
            // FileNotFoundException should be thrown here
            reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits());
            fail();
        } catch (RepositoryNotReachableException e) {
            fail();
        }
    }

    @Test(expected = SegmentNotFoundException.class)
    public void testMissingSegmentDetectedInFileStore() throws IOException, StorageException, URISyntaxException, InvalidFileStoreVersionException {

        AzurePersistence azurePersistence = new AzurePersistence(blobContainerClient, rootPrefix);
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(azurePersistence).build();

        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        //Assert.assertFalse(manager.exists("data00000a.tar"));
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
        writer.close();

        SegmentArchiveReader reader = manager.open("data00000a.tar");
        Buffer segment = reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits());
        assertNotNull(segment);

        deleteFirstWithPrefix("oak/data00000a.tar/0000.");

        // SegmentNotFoundException should be thrown here
        fileStore.readSegment(new SegmentId(fileStore, u.getMostSignificantBits(), u.getLeastSignificantBits()));
    }

    @Test
    public void testReadOnlyRecovery() throws URISyntaxException, InvalidFileStoreVersionException, IOException, CommitFailedException, StorageException {
        AzurePersistence rwPersistence = new AzurePersistence(blobContainerClient, rootPrefix);
        FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(rwPersistence).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        rwFileStore.flush();

        assertTrue(blobExists("oak/data00000a.tar"));
        assertFalse(blobExists("oak/data00000a.tar.ro.bak"));

        // create read-only FS
        AzurePersistence roPersistence = new AzurePersistence(blobContainerClient, rootPrefix);
        ReadOnlyFileStore roFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(roPersistence).buildReadOnly();

        PropertyState fooProperty = SegmentNodeStoreBuilders.builder(roFileStore).build()
                .getRoot()
                .getProperty("foo");
        assertThat(fooProperty, not(nullValue()));
        assertThat(fooProperty.getValue(Type.STRING), equalTo("bar"));

        roFileStore.close();
        rwFileStore.close();

        assertTrue(blobExists("oak/data00000a.tar"));
        // after creating a read-only FS, the recovery procedure should not be started since there is another running Oak process
        assertFalse(blobExists("oak/data00000a.tar.ro.bak"));
    }

    @Test
    public void testCachingPersistenceTarRecovery() throws URISyntaxException, InvalidFileStoreVersionException, IOException, CommitFailedException, StorageException {
        AzurePersistence rwPersistence = new AzurePersistence(blobContainerClient, rootPrefix);
        FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withCustomPersistence(rwPersistence).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        rwFileStore.flush();

        assertTrue(blobExists("oak/data00000a.tar"));
        assertFalse(blobExists("oak/data00000a.tar.ro.bak"));

        // create files store with split persistence
        AzurePersistence azureSharedPersistence = new AzurePersistence(blobContainerClient, rootPrefix);

        CachingPersistence cachingPersistence = new CachingPersistence(createPersistenceCache(), azureSharedPersistence);
        File localFolder = folder.newFolder();
        SegmentNodeStorePersistence localPersistence = new TarPersistence(localFolder);
        SegmentNodeStorePersistence splitPersistence = new SplitPersistence(cachingPersistence, localPersistence);

        // exception should not be thrown here
        FileStore splitPersistenceFileStore = FileStoreBuilder.fileStoreBuilder(localFolder).withCustomPersistence(splitPersistence).build();

        assertTrue(blobExists("oak/data00000a.tar"));
        // after creating a read-only FS, the recovery procedure should not be started since there is another running Oak process
        assertFalse(blobExists("oak/data00000a.tar.ro.bak"));
    }

    @Test
    public void testCollectBlobReferencesForReadOnlyFileStore() throws URISyntaxException, InvalidFileStoreVersionException, IOException, CommitFailedException, StorageException {
        AzurePersistence rwPersistence = new AzurePersistence(blobContainerClient, rootPrefix);
        try (FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(rwPersistence).build()) {
            SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
            NodeBuilder builder = segmentNodeStore.getRoot().builder();
            builder.setProperty("foo", "bar");
            segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            rwFileStore.flush();

            // file with binary references is not created yet
            assertFalse("brf file should not be present", blobExists("oak/data00000a.tar/data00000a.tar.brf"));

            // create read-only FS, while the rw FS is still open
            AzurePersistence roPersistence = new AzurePersistence(blobContainerClient, rootPrefix);
            try (ReadOnlyFileStore roFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(roPersistence).buildReadOnly()) {

                PropertyState fooProperty = SegmentNodeStoreBuilders.builder(roFileStore).build()
                        .getRoot()
                        .getProperty("foo");

                assertThat(fooProperty, not(nullValue()));
                assertThat(fooProperty.getValue(Type.STRING), equalTo("bar"));

                assertDoesNotThrow(() -> roFileStore.collectBlobReferences(s -> {
                }));
            }
        }
    }

    @Test
    public void testCollectBlobReferencesDoesNotFailWhenFileIsMissing() throws URISyntaxException, InvalidFileStoreVersionException, IOException, CommitFailedException, StorageException {
        AzurePersistence rwPersistence = new AzurePersistence(blobContainerClient, rootPrefix);
        try (FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(rwPersistence).build()) {
            SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
            NodeBuilder builder = segmentNodeStore.getRoot().builder();
            builder.setProperty("foo", "bar");
            segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            rwFileStore.flush();

            // file with binary references is not created yet
            assertFalse("brf file should not be present", blobExists("oak/data00000a.tar/data00000a.tar.brf"));

            // create read-only FS, while the rw FS is still open
            AzurePersistence roPersistence = new AzurePersistence(blobContainerClient, rootPrefix);
            try (ReadOnlyFileStore roFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(roPersistence).buildReadOnly()) {

                PropertyState fooProperty = SegmentNodeStoreBuilders.builder(roFileStore).build()
                        .getRoot()
                        .getProperty("foo");

                assertThat(fooProperty, not(nullValue()));
                assertThat(fooProperty.getValue(Type.STRING), equalTo("bar"));

                HashSet<String> references = new HashSet<>();
                assertDoesNotThrow(() ->
                        roFileStore.collectBlobReferences(references::add));

                assertTrue("No references should have been collected since reference file has not been created", references.isEmpty());
            }
        }
    }

    @Test
    public void testWriteAfterLosingRepoLock() throws Exception {
        AzurePersistence rwPersistence = new AzurePersistence(blobContainerClient, rootPrefix);

        BlockBlobClient blobClient = blobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();

        BlockBlobClient blobClientMocked = spy(blobClient);

        BlobLeaseClient leaseClientMocked = spy(createLeaseClient(blobClient));


        Mockito
                .doCallRealMethod()
                .when(leaseClientMocked).renewLeaseWithResponse((RequestConditions) Mockito.any(), Mockito.any(), Mockito.any());

        AzurePersistence mockedRwPersistence = Mockito.spy(rwPersistence);
        WriteAccessController writeAccessController = new WriteAccessController();
        AzureRepositoryLock azureRepositoryLock = new AzureRepositoryLock(blobClientMocked, leaseClientMocked, () -> {}, writeAccessController);
        AzureArchiveManager azureArchiveManager = new AzureArchiveManager(blobContainerClient, rootPrefix, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), writeAccessController);


        Mockito
                .doAnswer(invocation -> azureRepositoryLock.lock())
                .when(mockedRwPersistence).lockRepository();

        Mockito
                .doReturn(azureArchiveManager)
                .when(mockedRwPersistence).createArchiveManager(Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito
                .doReturn(new AzureJournalFile(blobContainerClient, rootPrefix + "/journal.log", writeAccessController))
                .when(mockedRwPersistence).getJournalFile();

        FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withCustomPersistence(mockedRwPersistence).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();


        // simulate operation timeout when trying to renew lease
        Mockito.reset(leaseClientMocked);

        BlobStorageException blobStorageException = spy(new BlobStorageException("operation timeout", null, null));
        Mockito.doReturn(BlobErrorCode.OPERATION_TIMED_OUT).when(blobStorageException).getErrorCode();

        Mockito.doThrow(blobStorageException).when(leaseClientMocked).renewLeaseWithResponse((RequestConditions) Mockito.any(), Mockito.any(), Mockito.any());


        // wait till lease expires
        Thread.sleep(17000);

        // try updating repository
        Thread thread = new Thread(() -> {
            try {
                builder.setProperty("foo", "bar");
                segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                rwFileStore.flush();
            } catch (Exception e) {
                fail("No Exception expected, but got: " + e.getMessage());
            }
        });
        thread.start();

        Thread.sleep(2000);

        // It should be possible to start another RW file store.
        FileStore rwFileStore2 = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withCustomPersistence(new AzurePersistence(blobContainerClient, rootPrefix)).build();
        SegmentNodeStore segmentNodeStore2 = SegmentNodeStoreBuilders.builder(rwFileStore2).build();
        NodeBuilder builder2 = segmentNodeStore2.getRoot().builder();

        //repository hasn't been updated
        assertNull(builder2.getProperty("foo"));

        rwFileStore2.close();

        Mockito.doCallRealMethod().when(leaseClientMocked).renewLeaseWithResponse((RequestConditions) Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testOpenWhenArchiveIsNotClosed() throws IOException, URISyntaxException {
        String segmentName = rootPrefix + "/data00000a.tar/0000." + UUID.randomUUID();
        blobContainerClient.getBlobClient(segmentName).getBlockBlobClient().upload(BinaryData.fromString("test"));

        AzureArchiveManager azureArchiveManager = new AzureArchiveManager(blobContainerClient, rootPrefix, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new WriteAccessController());

        assertNull(azureArchiveManager.open("data00000a.tar"));

    }

    @Test
    public void testListArchives() throws IOException {
        blobContainerClient.getBlobClient(rootPrefix + "/data00000a.tar/0000." + UUID.randomUUID()).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(rootPrefix + "/data00000a.tar/0001." + UUID.randomUUID()).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(rootPrefix + "/data00001a.tar/0000." + UUID.randomUUID()).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(rootPrefix + "/data00001a.tar/0001." + UUID.randomUUID()).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(rootPrefix + "/manifest").getBlockBlobClient().upload(BinaryData.fromString("test"));

        AzureArchiveManager azureArchiveManager = new AzureArchiveManager(blobContainerClient, rootPrefix, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new WriteAccessController());
        List<String> archives = azureArchiveManager.listArchives();
        assertEquals(2, archives.size());
        assertTrue(archives.contains("data00000a.tar"));
        assertTrue(archives.contains("data00001a.tar"));
    }

    @Test
    public void testDelete() {
        String segmentName0 = rootPrefix + "/data00000a.tar/0000." + UUID.randomUUID();
        String segmentName1 = rootPrefix + "/data00000a.tar/0001." + UUID.randomUUID();
        String segmentName2 = rootPrefix + "/data00000a.tar/0002." + UUID.randomUUID();

        blobContainerClient.getBlobClient(segmentName0).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(segmentName1).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(segmentName2).getBlockBlobClient().upload(BinaryData.fromString("test"));

        assertTrue(blobContainerClient.getBlobClient(segmentName0).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName1).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName2).getBlockBlobClient().exists());

        AzureArchiveManager azureArchiveManager = new AzureArchiveManager(blobContainerClient, rootPrefix, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new WriteAccessController());
        azureArchiveManager.delete("data00000a.tar");

        // verify that all segments are deleted
        assertFalse(blobContainerClient.getBlobClient(segmentName0).getBlockBlobClient().exists());
        assertFalse(blobContainerClient.getBlobClient(segmentName1).getBlockBlobClient().exists());
        assertFalse(blobContainerClient.getBlobClient(segmentName2).getBlockBlobClient().exists());
    }

    @Test
    public void testRenameTo() {
        String archiveNameFrom = "data00000a.tar";
        String archiveNameTo = "data00001a.tar";
        String segmentName0 = rootPrefix + "/" +archiveNameFrom+ "/0000." + UUID.randomUUID();
        String segmentName1 = rootPrefix + "/" +archiveNameFrom+ "/0001." + UUID.randomUUID();
        String segmentName2 = rootPrefix + "/" +archiveNameFrom+ "/0002." + UUID.randomUUID();

        blobContainerClient.getBlobClient(segmentName0).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(segmentName1).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(segmentName2).getBlockBlobClient().upload(BinaryData.fromString("test"));

        assertTrue(blobContainerClient.getBlobClient(segmentName0).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName1).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName2).getBlockBlobClient().exists());

        AzureArchiveManager azureArchiveManager = new AzureArchiveManager(blobContainerClient, rootPrefix, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new WriteAccessController());
        azureArchiveManager.renameTo(archiveNameFrom, archiveNameTo);

        // verify that all segments are deleted from old archive
        assertFalse(blobContainerClient.getBlobClient(segmentName0).getBlockBlobClient().exists());
        assertFalse(blobContainerClient.getBlobClient(segmentName1).getBlockBlobClient().exists());
        assertFalse(blobContainerClient.getBlobClient(segmentName2).getBlockBlobClient().exists());

        // verify that all segments are moved to new archive
        assertTrue(blobContainerClient.getBlobClient(segmentName0.replace(archiveNameFrom, archiveNameTo)).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName1.replace(archiveNameFrom, archiveNameTo)).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName2.replace(archiveNameFrom, archiveNameTo)).getBlockBlobClient().exists());
    }

    @Test
    public void testCopyFile() throws IOException {
        String archiveNameFrom = "data00000a.tar";
        String archiveNameTo = "data00001a.tar";
        String segmentName0 = rootPrefix + "/" +archiveNameFrom+ "/0000." + UUID.randomUUID();
        String segmentName1 = rootPrefix + "/" +archiveNameFrom+ "/0001." + UUID.randomUUID();
        String segmentName2 = rootPrefix + "/" +archiveNameFrom+ "/0002." + UUID.randomUUID();

        blobContainerClient.getBlobClient(segmentName0).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(segmentName1).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(segmentName2).getBlockBlobClient().upload(BinaryData.fromString("test"));

        assertTrue(blobContainerClient.getBlobClient(segmentName0).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName1).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName2).getBlockBlobClient().exists());

        AzureArchiveManager azureArchiveManager = new AzureArchiveManager(blobContainerClient, rootPrefix, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new WriteAccessController());
        azureArchiveManager.copyFile(archiveNameFrom, archiveNameTo);

        // verify that all segments are not deleted from old archive
        assertTrue(blobContainerClient.getBlobClient(segmentName0).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName1).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName2).getBlockBlobClient().exists());

        // verify that all segments are copied to new archive
        assertTrue(blobContainerClient.getBlobClient(segmentName0.replace(archiveNameFrom, archiveNameTo)).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName1.replace(archiveNameFrom, archiveNameTo)).getBlockBlobClient().exists());
        assertTrue(blobContainerClient.getBlobClient(segmentName2.replace(archiveNameFrom, archiveNameTo)).getBlockBlobClient().exists());
    }

    @Test
    public void testExists() {
        String archiveName = "data00000a.tar";
        String segmentName0 = rootPrefix + "/" +archiveName+ "/0000." + UUID.randomUUID();
        String segmentName1 = rootPrefix + "/" +archiveName+ "/0001." + UUID.randomUUID();

        blobContainerClient.getBlobClient(segmentName0).getBlockBlobClient().upload(BinaryData.fromString("test"));
        blobContainerClient.getBlobClient(segmentName1).getBlockBlobClient().upload(BinaryData.fromString("test"));

        AzureArchiveManager azureArchiveManager = new AzureArchiveManager(blobContainerClient, rootPrefix, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new WriteAccessController());

        assertTrue(azureArchiveManager.exists(archiveName));
    }

    private void deleteFirstWithPrefix(String prefix) {
        ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix(prefix);
        blobContainerClient.listBlobs(options, null).stream().findFirst().ifPresent(blobItem -> {
            blobContainerClient.getBlobClient(blobItem.getName()).getBlockBlobClient().delete();
        });
    }

    private void deleteBlob(String blobName) {
        blobContainerClient.getBlobClient(blobName).getBlockBlobClient().delete();
    }

    private boolean blobExists(String blobName) {
        ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix(blobName);
        return blobContainerClient.listBlobs(options, null).iterator().hasNext();
    }

    private BlobLeaseClient createLeaseClient(BlockBlobClient blobClient) {
        return new BlobLeaseClientBuilder()
                .blobClient(blobClient)
                .buildClient();
    }

    private PersistentCache createPersistenceCache() {
        return new AbstractPersistentCache() {
            @Override
            protected Buffer readSegmentInternal(long msb, long lsb) {
                return null;
            }

            @Override
            public boolean containsSegment(long msb, long lsb) {
                return false;
            }

            @Override
            public void writeSegment(long msb, long lsb, Buffer buffer) {

            }

            @Override
            public void cleanUp() {

            }
        };
    }

    private static void assertDoesNotThrow(Executable executable) {
        try {
            executable.execute();
        } catch (Exception e) {
            fail("No Exception expected, but got: " + e.getMessage());
        }
    }

    interface Executable {
        void execute() throws Exception;
    }
}
