package org.apache.jackrabbit.oak.store.remote;

import org.apache.jackrabbit.commons.cnd.CndImporter;
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
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeManager;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

public class RemoteNodeStoreRepoTest {

    RemoteNodeStore nodeStore;
    private Repository repository;
    MemoryStorage storage;

    static final String TEST_NODETYPES = "org/apache/jackrabbit/oak/store/remote/test_nodetypes.cnd";

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

        NodeTypeManager nodeTypeManager = session.getWorkspace().getNodeTypeManager();

        Reader cnd = new InputStreamReader(getClass().getClassLoader().getResourceAsStream(TEST_NODETYPES));
        CndImporter.registerNodeTypes(cnd, session);
        session.save();

        node = session.getRootNode().addNode("e", "nt:folder");
        //node.setProperty("jcr:primaryType", "test:aggregate");
        session.save();

//        NodeTypeDefinition nodeTypeDefinition =
//        nodeTypeManager.registerNodeType();
    }

    @Test
    public void testMove() throws RepositoryException {
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));

        session.getRootNode().addNode("a").addNode("b").addNode("c").addNode("d");
        session.getRootNode().addNode("e");
        session.save();

        session.move("/a/b/c", "/e/c");

        Node c = session.getNode("/e/c");
        Node d = session.getNode("/e/c/d");

        Assert.assertNotNull(c);
        Assert.assertNotNull(d);
    }
}
