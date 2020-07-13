package org.apache.jackrabbit.oak.store.remote;

import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.store.remote.store.MemoryStorage;
import org.junit.After;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeTypeManager;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class RemoteNodeStoreRepoTest {

    private static final String AGGREGATE = "test:aggregate";
    private static final String UNSTRUCTURED = "nt:unstructured";
    static final String TEST_NODETYPES = "org/apache/jackrabbit/oak/store/remote/test_nodetypes.cnd";

    RemoteNodeStore nodeStore;
    private Repository repository;
    MemoryStorage storage;
    private Session session;

    @Rule
    public TemporaryFolder blobStoreDir = new TemporaryFolder(new File("target"));

    @Before
    public void setUp() throws RepositoryException, IOException, ParseException {

        storage = new MemoryStorage();

        FileBlobStore fileBlobStore = new FileBlobStore(blobStoreDir.getRoot().getAbsolutePath());

        nodeStore = new RemoteNodeStore(storage, fileBlobStore);

        Jcr jcr = new Jcr(nodeStore);
        this.repository = jcr.createRepository();

        session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        Reader cnd = new InputStreamReader(getClass().getClassLoader().getResourceAsStream(TEST_NODETYPES));
        CndImporter.registerNodeTypes(cnd, session);
        session.save();
    }

    @After
    public void tearDown() {
        if(session != null) {
            session.logout();
        }
    }
    @Test
    public void test() throws RepositoryException, IOException, ParseException {

        Node node = session.getRootNode().addNode("a").addNode("b").addNode("c").addNode("d");

        node.setProperty("prop1", "val1");
        node.setProperty("prop2", "val2");

        session.save();
        session.logout();

        session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        node = session.getNode("/a/b/c/d");
        Assert.assertNotNull(node);
        Assert.assertEquals("val1", node.getProperty("prop1").getString());
        Assert.assertEquals("val2", node.getProperty("prop2").getString());

        session.save();
    }

    @Test
    public void testMove() throws RepositoryException {

        Node node = session.getRootNode().addNode("a").addNode("b").addNode("c").addNode("d");
        node.setProperty("prop1", "val1");
        node.setProperty("prop2", "val2");

        session.getRootNode().addNode("e");
        session.save();

        session.move("/a/b/c", "/e/c");

        session.save();

        Node c = session.getNode("/e/c");
        Node d = session.getNode("/e/c/d");

        Assert.assertNotNull(c);
        Assert.assertNotNull(d);

        Assert.assertEquals("val1", d.getProperty("prop1").getString());
        Assert.assertEquals("val2", d.getProperty("prop2").getString());
    }

    @Test
    public void testDelete() throws RepositoryException {

        session.getRootNode().addNode("a").addNode("b").addNode("c").addNode("d");

        session.save();

        Assert.assertTrue(session.nodeExists("/a/b/c"));
        Assert.assertTrue(session.nodeExists("/a/b/c/d"));

        Node node = session.getNode("/a/b/c");
        node.remove();

        session.save();

        Assert.assertFalse(session.nodeExists("/a/b/c"));
        Assert.assertFalse(session.nodeExists("/a/b/c/d"));

    }

    @Test
    public void testGetSubtree() throws RepositoryException, IOException, ParseException {

        Node node = session.getRootNode().addNode("a", UNSTRUCTURED).addNode("b", AGGREGATE).addNode("c", UNSTRUCTURED).addNode("d", UNSTRUCTURED);//.addNode("d", TYPE);
        node.setProperty("prop1", "val1");
        node.setProperty("prop2", "val2");
        session.save();

        Node b = session.getNode("/a/b");

        Node d = b.getNode("c/d");

        assertNotNull(d);

        b = d.getNode("../..");

        assertEquals("b", b.getName());

        Node a = d.getNode("../../..");

        assertEquals("a", a.getName());

        b.setProperty("bprop1", "bval1");

        //check before session save
        b = session.getNode("/a/b");
        assertEquals("bval1", b.getProperty("bprop1").getString());

        session.save();

        //check after session save
        b = session.getNode("/a/b");
        assertEquals("bval1", b.getProperty("bprop1").getString());
    }


    /*
            a
            |
            b    --> aggregate node, i.e. cq:Page of cq:PageContent
          /   \
        c      e
        |      |
        d      f

     */
    @Test
    public void testGetSubtree2() throws RepositoryException, IOException, ParseException {

        Node a = session.getRootNode().addNode("a", UNSTRUCTURED);
        Node b = a.addNode("b", AGGREGATE);
        b.addNode("c", UNSTRUCTURED).addNode("d", UNSTRUCTURED);
        b.addNode("e", UNSTRUCTURED).addNode("f", UNSTRUCTURED);

        session.save();

        b = session.getNode("/a/b");

        Node d = b.getNode("c/d");
        assertNotNull(d);
        assertEquals("d", d.getName());

        b = d.getNode("../..");
        assertEquals("b", b.getName());

        Node f = d.getNode("../../e/f");

        assertEquals("f", f.getName());

        b.setProperty("bprop1", "bval1");
        f.setProperty("fprop1", "fval1");

        //check before session save
        b = session.getNode("/a/b");
        f = b.getNode("e/f");
        assertEquals("bval1", b.getProperty("bprop1").getString());
        assertEquals("fval1", f.getProperty("fprop1").getString());

        session.save();

        //check after session save
        b = session.getNode("/a/b");
        f = b.getNode("e/f");
        assertEquals("bval1", b.getProperty("bprop1").getString());
        assertEquals("fval1", f.getProperty("fprop1").getString());
    }

    @Test
    public void testGetSubtreeAddChild() throws RepositoryException, IOException, ParseException {
        Node node = session.getRootNode().addNode("a", UNSTRUCTURED).addNode("b", AGGREGATE).addNode("c", UNSTRUCTURED).addNode("d", UNSTRUCTURED);
        session.save();

        Node b = session.getNode("/a/b");

        b.addNode("e", UNSTRUCTURED);

        Node e = b.getNode("e");
        e.setProperty("eprop1", "eval1");

        assertNotNull(e);
        //jcr:primaryType should be set
        assertNotNull(e.getProperty("jcr:primaryType"));
        assertEquals("eval1", e.getProperty("eprop1").getString());

        session.save();

        e = session.getNode("/a/b/e");
        assertNotNull(e);
        assertNotNull(e.getProperty("jcr:primaryType"));
        assertEquals("eval1", e.getProperty("eprop1").getString());
    }

    @Test
    public void testBlobStore() throws RepositoryException {

        Node a = session.getRootNode().addNode("a", UNSTRUCTURED);

        ValueFactory factory = session.getValueFactory();
        InputStream is = new ByteArrayInputStream("binary value to become".getBytes());

        Binary binary = factory.createBinary(is);
        Value value = factory.createValue(binary);
        a.setProperty("jcr:data", value);

        session.save();

        assertEquals(binary, a.getProperty("jcr:data").getBinary());

    }
}
