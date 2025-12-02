package org.apache.jackrabbit.oak.security.authentication.sync;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;

public class UserSyncConsumer {

    private final String instanceId;

    private static final Logger LOG = LoggerFactory.getLogger(UserSyncConsumer.class);

    private final UserManager userManager;
    private final JackrabbitSession session;

    private String lastProcessedChange;

    private BlobStore blobStore;

    public UserSyncConsumer(String instanceId, JackrabbitSession session, UserDiffPayloadQueue payloadQueue, BlobStore blobStore) throws RepositoryException {
        this.instanceId = instanceId;
        this.session = session;
        this.userManager = session.getUserManager() ;
        payloadQueue.registerQueueListener(this::processChange);
        this.blobStore = blobStore;
    }

    public void processChange(String change) {
        this.lastProcessedChange = change;
        try {
            LOG.info("Processing change {}", change);
            ParseJsopUserManagerDiff.applyJsopDiff(change, session, blobStore);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    public String getLastProcessedChange() {
        return lastProcessedChange;
    }

    public String getInstanceId() {
        return instanceId;
    }
}
