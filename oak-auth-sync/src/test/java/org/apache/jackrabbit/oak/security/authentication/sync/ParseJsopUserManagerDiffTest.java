package org.apache.jackrabbit.oak.security.authentication.sync;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.user.UserManagerImpl;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import static org.junit.Assert.*;

public class ParseJsopUserManagerDiffTest {
    private Repository repository;
    private JackrabbitSession session;
    private UserManager userManager;
    private BlobStore blobStore = new MemoryBlobStore(); // Use a real BlobStore implementation if needed

    @Before
    public void setUp() throws Exception {
        repository = new Jcr().createRepository();
        session = (JackrabbitSession) repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        userManager = session.getUserManager();
        blobStore = null; // Initialize with a real BlobStore if needed
    }

    @After
    public void tearDown() throws Exception {
        if (session != null) session.logout();
    }

    @Test
    public void testAddUserAndModifyProperty() throws Exception {
        String jsop = "+\"/rep:security/rep:authorizables/rep:users/t\":{\"jcr:primaryType\":\"nam:rep:AuthorizableFolder\",\"te\":{\"jcr:primaryType\":\"nam:rep:AuthorizableFolder\",\"testUser\":{\"jcr:created\":\"dat:2025-06-12T16:22:01.016+02:00\",\"rep:authorizableId\":\"testUser\",\"jcr:createdBy\":\"admin\",\"jcr:primaryType\":\"nam:rep:User\",\"jcr:uuid\":\"5d9c68c6-c50e-33d0-aa2f-cf54f63993b6\",\"rep:password\":\"{SHA-256}b6893ead85427bc9-1000-a8a9c7cc6402f060dae79017394f9331394951a6eb1e77a73e5adb316c4efa0f\",\"email\":\"test@test.com\",\"rep:principalName\":\"testUser\"}}}";
        ParseJsopUserManagerDiff.applyJsopDiff(jsop, session, blobStore);
        session.save();

        User testUser = (User) userManager.getAuthorizable("testUser");

        assertNotNull("User should be created", testUser);
        assertEquals("testUser", testUser.getID());
        assertEquals("test@test.com", testUser.getProperty("email")[0].getString());

        jsop = "^\"/rep:security/rep:authorizables/rep:users/t/te/testUser/email\":\"testch1@test.com\"";

        ParseJsopUserManagerDiff.applyJsopDiff(jsop, session, blobStore);

        session.save();

        testUser = (User) userManager.getAuthorizable("testUser");
        assertNotNull("User should still exist after property modification", testUser);
        assertEquals("testch1@test.com", testUser.getProperty("email")[0].getString());
    }

    @Test
    public void testAddGroup() throws Exception {
        String jsop = "+\"/rep:security/rep:authorizables/rep:groups\":{\"jcr:primaryType\":\"nam:rep:AuthorizableFolder\",\"t\":{\"jcr:primaryType\":\"nam:rep:AuthorizableFolder\",\"te\":{\"jcr:primaryType\":\"nam:rep:AuthorizableFolder\",\"testGroup\":{\"jcr:created\":\"dat:2025-06-12T16:54:26.438+02:00\",\"rep:authorizableId\":\"testGroup\",\"jcr:createdBy\":\"admin\",\"jcr:primaryType\":\"nam:rep:Group\",\"jcr:uuid\":\"9628ffae-cf05-3138-8185-2cb572d50d45\",\"rep:principalName\":\"testGroup\"}}}}";
        ParseJsopUserManagerDiff.applyJsopDiff(jsop, session, blobStore);
        session.save();

        Group testGroup = (Group) userManager.getAuthorizable("testGroup");

        assertNotNull("Group should be created", testGroup);
        assertEquals("testGroup", testGroup.getID());
    }

    @Test
    public void testAddMember() throws Exception {
        User testUser = userManager.createUser("testUser", "test");
        userManager.createGroup("testGroup");
        session.save();

        // Add user to group
        String jsop = "^\"/rep:security/rep:authorizables/rep:groups/t/te/testGroup/rep:members\":[\"+testUser\"]";

        ParseJsopUserManagerDiff.applyJsopDiff(jsop, session, blobStore);
        session.save();

        Group testGroup = (Group) userManager.getAuthorizable("testGroup");
        assertNotNull("Group should still exist after adding member", testGroup);
        assertTrue("User should be a member of the group", testGroup.isMember(testUser));

        // Add another user to the group
        User anotherUser = userManager.createUser("testUser2", "test2");
        session.save();

        jsop = "^\"/rep:security/rep:authorizables/rep:groups/t/te/testGroup/rep:members\":[\"+testUser2\"]";

        ParseJsopUserManagerDiff.applyJsopDiff(jsop, session, blobStore);
        session.save();

        assertTrue("Another user should also be a member of the group", testGroup.isMember(anotherUser));
        assertTrue("First user should also be a member of the group", testGroup.isMember(testUser));

        // Remove user from group
        jsop = "^\"/rep:security/rep:authorizables/rep:groups/t/te/testGroup/rep:members\":[\"-testUser\"]";
        ParseJsopUserManagerDiff.applyJsopDiff(jsop, session, blobStore);
        session.save();

        assertFalse("User should no longer be a member of the group", testGroup.isMember(testUser));
        assertTrue("Another user should still be a member of the group", testGroup.isMember(anotherUser));
    }

    @Test
    public void testCreateUserAndAddItToTheGroup() throws RepositoryException {
        Group testGroup = userManager.createGroup("testGroup");
        session.save();

        String jsop = "+\"/rep:security/rep:authorizables/rep:users/t/te/testUser\":{\"jcr:created\":\"dat:2025-06-16T11:50:56.244+02:00\",\"rep:authorizableId\":\"testUser\",\"jcr:createdBy\":\"admin\",\"jcr:primaryType\":\"nam:rep:User\",\"jcr:uuid\":\"1e4332f6-5a7a-3210-b5fb-fb92c7c60cce\",\"rep:password\":\"{SHA-256}c6d6363b98789bb2-1000-01901eaa1bdffe595aafe43ae4801592effdfceb68d7724e962e745f24eaa6e1\",\"email\":\"testUser@test.com\",\"rep:principalName\":\"testUser\"}^\"/rep:security/rep:authorizables/rep:groups/t/te/testGroup/rep:members\":[\"+testUser\"]";

        ParseJsopUserManagerDiff.applyJsopDiff(jsop, session, blobStore);
        session.save();

        User testUser = (User) userManager.getAuthorizable("testUser");
        assertNotNull("User should be created", testUser);
        assertEquals("testUser", testUser.getID());
        assertEquals("testUser@test.com", testUser.getProperty("email")[0].getString());
        assertTrue("User should be a member of the group", testGroup.isMember(testUser));
    }

    @Test
    public void testAddGroupAndTwoUsersAtOnce() throws Exception {
        //TODO
    }
} 