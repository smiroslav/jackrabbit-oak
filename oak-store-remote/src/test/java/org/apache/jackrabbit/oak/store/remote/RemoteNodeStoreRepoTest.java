package org.apache.jackrabbit.oak.store.remote;

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
    public void test() throws RepositoryException {
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        Node node = session.getRootNode().addNode("a");//.addNode("b").addNode("c").addNode("d");

        node.setProperty("prop1", "val1");
        node.setProperty("prop2", "val2");

        session.save();
        session.logout();

        session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        node = session.getNode("/a");
        Assert.assertNotNull(node);
        Assert.assertEquals("val1", node.getProperty("prop1").getString());
        Assert.assertEquals("val2", node.getProperty("prop2").getString());

    }
}
