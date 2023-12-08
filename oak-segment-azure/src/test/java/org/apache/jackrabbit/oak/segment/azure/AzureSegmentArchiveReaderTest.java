package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class AzureSegmentArchiveReaderTest {
    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient blobContainerClient;

    private String rootPrefix = "oak";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setUp() throws Exception {
        blobContainerClient = azurite.getBlobContainerClient("oak-test");
    }


    @Test
    public void testDoReadSegmentToBuffer() throws Exception {
        String archiveName = "data00000a.tar";
        String segmentName0 = "0000." + UUID.randomUUID();
        String segmentName1 = "0001." + UUID.randomUUID();
        String segmentContent0 = "segment0000";
        String segmentContent1 = "segment0001";
        blobContainerClient.getBlobClient(getArchivePrefix(archiveName) + segmentName0).getBlockBlobClient().upload(BinaryData.fromString(segmentContent0));
        blobContainerClient.getBlobClient(getArchivePrefix(archiveName)+ segmentName1).getBlockBlobClient().upload(BinaryData.fromString(segmentContent1));

        AzureSegmentArchiveReader reader = new AzureSegmentArchiveReader(blobContainerClient, rootPrefix, archiveName, null);

        // read first segment
        Buffer buffer = Buffer.wrap(segmentContent0.getBytes());
        Buffer buffer0 = Buffer.allocate(buffer.capacity());
        reader.doReadSegmentToBuffer(segmentName0, buffer0);
        assertEquals(buffer, buffer0);

        // read second segment
        buffer = Buffer.wrap(segmentContent1.getBytes());
        Buffer buffer1 = Buffer.allocate(buffer.capacity());
        reader.doReadSegmentToBuffer(segmentName1, buffer1);
        assertEquals(buffer, buffer1);
    }

    @Test
    public void testGetName() throws IOException {
        String archiveName = "data00000a.tar";

        AzureSegmentArchiveReader reader = new AzureSegmentArchiveReader(blobContainerClient, rootPrefix, archiveName, null);

        assertEquals(archiveName, reader.getName());
    }

    @Test
    public void testDoReadDataFile() throws IOException {
        String archiveName = "data00000a.tar";
        String graphFileName = archiveName + ".gph";
        String binaryRefsFileName = archiveName + ".brf";

        blobContainerClient.getBlobClient(getArchivePrefix(archiveName) + graphFileName).getBlockBlobClient().upload(BinaryData.fromString("graph"));
        blobContainerClient.getBlobClient(getArchivePrefix(archiveName) + binaryRefsFileName).getBlockBlobClient().upload(BinaryData.fromString("binaryRefs"));

        AzureSegmentArchiveReader reader = new AzureSegmentArchiveReader(blobContainerClient, rootPrefix, archiveName, null);
        Buffer graph = reader.doReadDataFile(".gph");
        assertEquals("graph", new String(graph.array()));

        Buffer binaryRefs = reader.doReadDataFile(".brf");
        assertEquals("binaryRefs", new String(binaryRefs.array()));
    }

    @NotNull
    private String getArchivePrefix(String archiveName) {
        return rootPrefix + "/" + archiveName + "/";
    }
}
