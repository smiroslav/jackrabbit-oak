package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.LargeNumberOfChildNodeUpdatesIT;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.vm.VM;

import java.io.File;
import java.io.IOException;

import static java.lang.System.getProperty;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeTrue;

public class SegmentNodeStateTest {

    public static void main(String[] args) throws Exception {
        System.out.println(VM.current().details());
        System.out.println(ClassLayout.parseClass(SegmentNodeState.class).toPrintable());

    }

    public static class A {
        boolean f;
    }

    /** Only run if explicitly asked to via -Dtest=LargeNumberOfChildNodeUpdatesIT */
    private static final boolean ENABLED =
            LargeNumberOfChildNodeUpdatesIT.class.getSimpleName().equals(getProperty("test"));

    private static final int NODE_COUNT = Integer
            .getInteger("LargeNumberOfChildNodeUpdatesIT.child-count", 5000000);

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        assumeTrue(ENABLED);
    }

    @Test
    public void testNode() throws IOException, InvalidFileStoreVersionException {
        try (FileStore fileStore = FileStoreBuilder.fileStoreBuilder(folder.getRoot()).build()) {
            DefaultSegmentWriter writer = defaultSegmentWriterBuilder("test")
                    .withGeneration(GCGeneration.newGCGeneration(1, 1, false))
                    .build(fileStore);

            SegmentNodeState root = fileStore.getHead();
            SegmentNodeBuilder builder = root.builder();
            for (int k = 0; k < NODE_COUNT; k++) {
                builder.setChildNode("n-" + k);
            }

            SegmentNodeState node1 = builder.getNodeState();
            RecordId nodeId = writer.writeNode(node1);
            SegmentNodeState node2 = fileStore.getReader().readNode(nodeId);

            assertNotEquals(node1.getRecordId(), node2.getRecordId());
            assertEquals(node1, node2);
        }
    }
}
