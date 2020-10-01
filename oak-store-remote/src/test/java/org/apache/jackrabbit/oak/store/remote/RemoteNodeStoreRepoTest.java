package org.apache.jackrabbit.oak.store.remote;

import org.apache.commons.lang.CharEncoding;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
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
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class RemoteNodeStoreRepoTest extends AbstractRemoteNodeStoreRepoTest{

    MemoryStorage storage;


    @Before
    public void setUp() throws RepositoryException, IOException, ParseException {

        storage = new MemoryStorage();

        fileBlobStore = new FileBlobStore(blobStoreDir.getRoot().getAbsolutePath());

        nodeStore = new RemoteNodeStore(storage, fileBlobStore);

        super.setUp();
    }

    @After
    public void tearDown() {
        if(session != null) {
            session.logout();
        }
    }

    static final String SITEMAP_NAMESPACE = "http://www.sitemaps.org/schemas/sitemap/0.9";

    @Test
    public void test3() throws RepositoryException, IOException, ParseException {

        Node node = session.getRootNode().addNode("a");

        PipedInputStream in = new PipedInputStream();
        PipedOutputStream out = new PipedOutputStream(in);

        new Thread(
                new Runnable(){
                    public void run(){
                        try {
                            XMLStreamWriter xmlWriter = XMLOutputFactory.newFactory().createXMLStreamWriter(out, CharEncoding.UTF_8);

                            xmlWriter.writeStartDocument(CharEncoding.UTF_8, "1.0");
                            xmlWriter.writeStartElement("", "sitemapindex", SITEMAP_NAMESPACE);

                            xmlWriter.writeEndElement();

                            xmlWriter.writeEndDocument();
                            out.close();

                        } catch (XMLStreamException | IOException e) {
                            e.printStackTrace();
                        }

                    }
                }
        ).start();

        node.setProperty("binaryProp",session.getValueFactory().createBinary(in));
        in.close();
        session.save();

    }

}
