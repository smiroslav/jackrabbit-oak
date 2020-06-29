package org.apache.jackrabbit.oak.jcr.mstests;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.GuestCredentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class AppTest {

    private FileStore store;

    private Repository repository;

    @Rule
    public TemporaryFolder root = new TemporaryFolder();

    @Before
    public void before() throws Exception {
//        File directory = new File("target", "segment-tar-" + System.currentTimeMillis());
//        this.store = FileStoreBuilder.fileStoreBuilder(directory).withMaxFileSize(1).build();
//        Jcr jcr = new Jcr(new Oak(SegmentNodeStoreBuilders.builder(store).build()));
        Jcr jcr = new Jcr(new MemoryNodeStore());
        this.repository = jcr.createRepository();
    }
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() throws RepositoryException {
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));


        Node node = session.getRootNode().addNode("a").addNode("b").addNode("c").addNode("d");

        node.setProperty("prop1", "val1");
        node.setProperty("prop2", "val2");

        session.save();

        node = session.getNode("/a/b/c/d");

        assertEquals("val1", node.getProperty("prop1").getString());
        assertEquals("val2", node.getProperty("prop2").getString());

        node.setProperty("prop3", "val3");

        assertNotNull(node);


        session.save();
        node = session.getNode("/a/b/c/d");

        assertEquals("val3", node.getProperty("prop3").getString());

    }


}
