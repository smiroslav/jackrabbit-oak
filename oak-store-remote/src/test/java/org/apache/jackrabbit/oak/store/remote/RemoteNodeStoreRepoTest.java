package org.apache.jackrabbit.oak.store.remote;

import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.store.remote.store.MemoryStorage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import java.io.IOException;

public class RemoteNodeStoreRepoTest {

    RemoteNodeStore nodeStore;
    private Repository repository;
    MemoryStorage storage;

    @Before
    public void setUp() {

        storage = new MemoryStorage();

        nodeStore = new RemoteNodeStore(storage, null);

        Jcr jcr = new Jcr(nodeStore);
        this.repository = jcr.createRepository();
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
}
