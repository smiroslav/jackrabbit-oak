package org.apache.jackrabbit.oak.kv.store;

import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.kv.KVNodeStore;
import org.apache.jackrabbit.oak.kv.store.memory.MemoryStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KVNodeStoreTest {

    private Store store;
    private KVNodeStore nodeStore;

    private Repository repository;

    @Before
    public void setUp() throws Exception {
        store = new MemoryStore();
        nodeStore = new KVNodeStore(store, null);

        Jcr jcr = new Jcr(nodeStore);
        this.repository = jcr.createRepository();
    }

    @Test
    public void test() throws RepositoryException {
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        Node node = session.getRootNode().addNode("a");//.addNode("b").addNode("c").addNode("d");

        node.setProperty("prop1", "val1");
        node.setProperty("prop2", "val2");

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
    public void testNodeStore() throws RepositoryException {
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        Node node = session.getRootNode().addNode("a");//.addNode("b").addNode("c").addNode("d");

        node.setProperty("prop1", "val1");
        node.setProperty("prop2", "val2");

        session.save();
        session.logout();


        session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        node = session.getNode("/a/b/c/d");


        assertNotNull(node);
        assertEquals("val1", node.getProperty("prop1").getString());
        assertEquals("val2", node.getProperty("prop2").getString());

        node.setProperty("prop3", "val3");

        session.save();

        session.getRootNode().addNode("e");

        session.save();

        session.move("/a/b", "/e/b");

        session.save();

        node = session.getNode("/e/b/c/d");

        assertNotNull(node);
    }
}
