package org.apache.jackrabbit.oak.store.remote;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.TreeNode;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.store.remote.store.MemoryStorage;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RemoteNodeStateTest {

    MemoryStorage storage = null;

    @Before
    public void setUp() {
        storage = new MemoryStorage();
    }

    @Test
    public void testExist() {
        List<PropertyState> props = new ArrayList<>();
        PropertyState p1 = new StringPropertyState("prop1", "val1");
        PropertyState p2 = new StringPropertyState("prop2", "val2");
        props.add(p1);
        props.add(p2);

        storage.addNode("/a/b/c", props);

        RemoteNodeState nodeState = new RemoteNodeState("/a/b/c", storage, null, 1);

        assertTrue(nodeState.exists());

        List<PropertyState> propertyStates =  StreamSupport.stream(nodeState.getProperties().spliterator(), false).collect(Collectors.toList());

        assertEquals(2, propertyStates.size());
        assertTrue(propertyStates.contains(p1));
        assertTrue(propertyStates.contains(p2));



        //should be possible to retrieve the node if it is not deleted even when revision increases

        nodeState = new RemoteNodeState("/a/b/c", storage, null, 2);

        assertTrue(nodeState.exists());

        propertyStates =  StreamSupport.stream(nodeState.getProperties().spliterator(), false).collect(Collectors.toList());

        assertEquals(2, propertyStates.size());
        assertTrue(propertyStates.contains(p1));
        assertTrue(propertyStates.contains(p2));

        //delete node and check if it is visible in the next revision
        storage.deleteNode("/a/b/c", 3);

        nodeState = new RemoteNodeState("/a/b/c", storage, null, 4);

        assertFalse(nodeState.exists());
    }

    @Test
    public void testChildren() {
        storage.addNode("/a", Collections.emptyList());
        storage.incrementRevisionNumber();
        storage.addNode("/a/b", Collections.emptyList());
        storage.addNode("/a/b/c", Collections.emptyList());
        storage.incrementRevisionNumber();
        storage.addNode("/a/d", Collections.emptyList());
        storage.incrementRevisionNumber();
        storage.deleteNode("/a/b", 4);
        storage.deleteNode("/a/b/c", 4);
        storage.incrementRevisionNumber();

        RemoteNodeState ab5 = new RemoteNodeState("/a/b", storage, null, 5);
        RemoteNodeState ab2 = new RemoteNodeState("/a/b", storage, null, 2);
        assertTrue(ab2.exists());
        assertFalse(ab5.exists());

        TreeMap<String, Node> childNodes = storage.getNodeAndSubtree("/a", 3, false);
        assertEquals(3, childNodes.size());
        assertTrue(childNodes.containsKey("/a"));
        assertTrue(childNodes.containsKey("/a/b"));
        assertTrue(childNodes.containsKey("/a/d"));

        childNodes = storage.getNodeAndSubtree("/a", 4, false);
        assertEquals(2, childNodes.size());
        assertTrue(childNodes.containsKey("/a"));
        assertTrue(childNodes.containsKey("/a/d"));

        RemoteNodeState a3 = new RemoteNodeState("/a", storage, null, 3);
        List<ChildNodeEntry> childNodeEntries = StreamSupport.stream(a3.getChildNodeEntries().spliterator(), false).collect(Collectors.toList());
        assertEquals(2, childNodeEntries.size());

        assertNotNull(a3.getChildNode("b"));
        assertNotNull(a3.getChildNode("d"));
        assertTrue(a3.hasChildNode("b"));
        assertTrue(a3.hasChildNode("d"));

        RemoteNodeState a4 = new RemoteNodeState("/a", storage, null, 4);
        childNodeEntries = StreamSupport.stream(a4.getChildNodeEntries().spliterator(), false).collect(Collectors.toList());

        assertEquals(1, childNodeEntries.size());
    }

    @Test
    public void testMove() {
        storage.addNode("/a", Collections.emptyList());
        storage.addNode("/a/b", Collections.emptyList());
        storage.addNode("/a/b/c", Collections.emptyList());
        storage.addNode("/a/b/c/d", Collections.emptyList());
        storage.addNode("/a/b/c/e", Collections.emptyList());
        storage.addNode("/a/b/c/f", Collections.emptyList());
        storage.addNode("/g", Collections.emptyList());

        storage.moveChildNodes("/a/b/c", "/g/c");

        TreeMap<String, Node> tree = storage.getNodeAndSubtree("/g", 1, true);

        assertEquals(4, tree.size());
    }

    @Test
    public void testCompareAgainstBaseState(){
        List<PropertyState> props = new ArrayList<>();
        PropertyState p1 = new StringPropertyState("prop1", "val1");
        PropertyState p2 = new StringPropertyState("prop2", "val2");
        props.add(p1);
        props.add(p2);

        //revision 1
        storage.addNode("/a", Collections.emptyList());
        storage.addNode("/a/b", props);
        storage.addNode("/a/b/c", Collections.emptyList());
        storage.incrementRevisionNumber();

        //revision 2
        storage.deleteNode("/a/b/c");

        props = new ArrayList<>();
        PropertyState p3 = new StringPropertyState("prop3", "val3");
        p2 = new StringPropertyState("prop2", "val2");
        props.add(p3);
        props.add(p2);
        storage.addNode("/a/b", props);
        storage.addNode("/a/d", Collections.emptyList());


        List<String> propertiesAdded = new ArrayList<>();
        List<String> propertiesDeleted = new ArrayList<>();
        List<String> nodesDeleted = new ArrayList<>();
        List<String> nodesChanged = new ArrayList<>();
        List<String> nodesAdded = new ArrayList<>();

        NodeStateDiff nodeStateDiff = new NodeStateDiff() {
            @Override
            public boolean propertyAdded(PropertyState after) {
                propertiesAdded.add(after.getName());
                return true;
            }

            @Override
            public boolean propertyChanged(PropertyState before, PropertyState after) {
                return true;
            }

            @Override
            public boolean propertyDeleted(PropertyState before) {
                propertiesDeleted.add(before.getName());
                return true;
            }

            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                nodesAdded.add(name);
                return true;
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                nodesChanged.add(name);
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                nodesDeleted.add(name);
                return true;
            }
        };

        RemoteNodeState a1 = new RemoteNodeState("/a", storage, null, 1);
        RemoteNodeState a2 = new RemoteNodeState("/a", storage, null, 2);

        a2.compareAgainstBaseState(a1, nodeStateDiff);
        assertTrue(nodesAdded.contains("d"));
        assertTrue(nodesChanged.contains("b"));
        assertTrue(nodesDeleted.isEmpty());

        RemoteNodeState b1 = new RemoteNodeState("/a/b", storage, null, 1);
        RemoteNodeState b2 = new RemoteNodeState("/a/b", storage, null, 2);

        b2.compareAgainstBaseState(b1, nodeStateDiff);
        assertTrue(nodesDeleted.contains("c"));
        assertTrue(propertiesAdded.contains("prop3"));
        assertTrue(propertiesDeleted.contains("prop1"));


    }

    @Test
    public void testLoadSubtree() {
        storage.addNode("/a", Collections.emptyList());
        storage.addNode("/a/b", Collections.emptyList());
        storage.addNode("/a/b/c", Collections.emptyList());
        storage.addNode("/a/b/c/d", Collections.emptyList());
        storage.addNode("/a/b/c/e", Collections.emptyList());
        storage.addNode("/a/b/c/f", Collections.emptyList());
        storage.addNode("/a/b/g", Collections.emptyList());
        storage.addNode("/a/1", Collections.emptyList());
        storage.addNode("/a/1/2", Collections.emptyList());
        storage.addNode("/a/1/2/3", Collections.emptyList());
        storage.addNode("/a/4", Collections.emptyList());
        storage.addNode("/a/4/5", Collections.emptyList());

        TreeMap<String, Node> tree = storage.getNodeAndSubtree("/a", 1, true);

        RemoteNodeState b1 = new RemoteNodeState("/a/b", storage, null, 4);

        TreeNode subtree =  b1.loadSubtree();

        assertNotNull(subtree);
        assertEquals("b", subtree.getName());
        assertEquals(2, subtree.getChildren().size());

        TreeNode c = subtree.getChildren().get("c");
        TreeNode g = subtree.getChildren().get("c");

        assertNotNull(c);
        assertNotNull(g);

        assertEquals(3, c.getChildren().size());

        TreeNode d = c.getChildren().get("d");
        TreeNode e = c.getChildren().get("e");
        TreeNode f = c.getChildren().get("f");

        assertNotNull(d);
        assertNotNull(e);
        assertNotNull(f);
    }
}
