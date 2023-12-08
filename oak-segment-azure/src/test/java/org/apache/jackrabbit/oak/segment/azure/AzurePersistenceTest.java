package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class AzurePersistenceTest {

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
    public void testSegmentFilesExist() {

        String archiveName = "data00000a.tar";
        String segmentName0 = "0000." + UUID.randomUUID();
        String segmentName1 = "0001." + UUID.randomUUID();

        blobContainerClient.getBlobClient(getArchivePrefix(archiveName) + segmentName0).getBlockBlobClient().upload(BinaryData.fromString("segmentContent0"));
        blobContainerClient.getBlobClient(getArchivePrefix(archiveName) + segmentName1).getBlockBlobClient().upload(BinaryData.fromString("segmentContent1"));

        AzurePersistence azurePersistence = new AzurePersistence(blobContainerClient, rootPrefix);

        assertTrue(azurePersistence.segmentFilesExist());
    }

    private String getArchivePrefix(String archiveName) {
        return rootPrefix + "/" + archiveName + "/";
    }
}
