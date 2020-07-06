package org.apache.jackrabbit.oak.store.remote;

import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.store.remote.store.MemoryStorage;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.nodetype.NodeTypeManager;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

public class RemoteNodeStoreRepoTest {

    private static final String AGGREGATE = "test:aggregate";
    private static final String UNSTRUCTURED = "nt:unstructured";
    static final String TEST_NODETYPES = "org/apache/jackrabbit/oak/store/remote/test_nodetypes.cnd";

    RemoteNodeStore nodeStore;
    private Repository repository;
    MemoryStorage storage;

    @Before
    public void setUp() throws RepositoryException, IOException, ParseException {

        storage = new MemoryStorage();

        nodeStore = new RemoteNodeStore(storage, null);

        Jcr jcr = new Jcr(nodeStore);
        this.repository = jcr.createRepository();

        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        Reader cnd = new InputStreamReader(getClass().getClassLoader().getResourceAsStream(TEST_NODETYPES));
        CndImporter.registerNodeTypes(cnd, session);
        session.save();
    }
    @Test
    public void test() throws RepositoryException, IOException, ParseException {
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        Node node = session.getRootNode().addNode("a").addNode("b").addNode("c").addNode("d");//.addNode("b").addNode("c").addNode("d");

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
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

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
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

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

        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        Node node = session.getRootNode().addNode("a", UNSTRUCTURED).addNode("b", AGGREGATE).addNode("c", UNSTRUCTURED).addNode("d", UNSTRUCTURED);//.addNode("d", TYPE);
        node.setProperty("prop1", "val1");
        node.setProperty("prop2", "val2");
        session.save();

        node = session.getNode("/a/b");

        Node d = node.getNode("c/d");

        assertNotNull(d);
    }

//    @Test
//    public void testNodeT() throws RepositoryException, IOException, ParseException {
//        Session session = repository.login(
//                new SimpleCredentials("admin", "admin".toCharArray()));
//
//        Reader cnd = new InputStreamReader(getClass().getClassLoader().getResourceAsStream(TEST_NODETYPES));
//        CndImporter.registerNodeTypes(cnd, session);
//        session.save();
//
//        Node node = session.getRootNode().addNode("e", AGGREGATE);
//        //node.setProperty("jcr:primaryType", "test:aggregate");
//        session.save();
//
//        node = session.getNode("/e");
//
//        assertEquals("test:aggregate", node.getProperty("jcr:primaryType").getString());
//    }
}
