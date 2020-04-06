package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

import javax.jcr.*;
import java.util.HashSet;
import java.util.Set;

public class CacheImprovementTest {

    @Rule
    public TemporaryFolder root = new TemporaryFolder();

    private FileStore newFileStore() throws Exception {
        return FileStoreBuilder.fileStoreBuilder(root.getRoot()).build();
    }

    @Test
    public void testCaches() throws Exception {
        try (FileStore store = newFileStore()) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(store).build();
            Repository repo = new Jcr(new Oak(ns)).createRepository();



            Session session = repo.login(
                    new SimpleCredentials("admin", "admin".toCharArray()));
            Node root = session.getRootNode();

            root.addNode("a");
            root.addNode("a/b");
            root.addNode("a/b/c");

            Node c = root.getNode("a/b/c");

            c.setProperty("1", "c1");
            c.setProperty("2", "c2");
            c.setProperty("3", "c3");

            session.save();

            session.logout();

            session = repo.login(
                    new SimpleCredentials("admin", "admin".toCharArray()));
            root = session.getRootNode();

            c = root.getNode("a/b/c");

            assertNotNull(c);

            Property prop1 = c.getProperty("1");
            assertEquals("c1", prop1.getString());

            Property prop2 = c.getProperty("2");
            assertEquals("c2", prop2.getString());

            Property prop3 = c.getProperty("3");
            assertEquals("c3", prop3.getString());

            session.logout();

            session = repo.login(
                    new SimpleCredentials("admin", "admin".toCharArray()));
            root = session.getRootNode();

            c = root.getNode("a/b/c");

            PropertyIterator propertyIterator = c.getProperties();

            Set<String> values = new HashSet<>();
            values.add("c1");
            values.add("c2");
            values.add("c3");

            while (propertyIterator.hasNext()) {
                Property property = propertyIterator.nextProperty();

                if(values.contains(property.getString())) {
                    values.remove(property.getString());
                }
            }
            assertTrue(values.isEmpty());
        }
    }

}