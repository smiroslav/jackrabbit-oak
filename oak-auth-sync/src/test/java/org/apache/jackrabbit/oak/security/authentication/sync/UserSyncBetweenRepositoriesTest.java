/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.security.authentication.sync;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Repository;
import javax.jcr.SimpleCredentials;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class UserSyncBetweenRepositoriesTest {

    private static final Logger LOG = LoggerFactory.getLogger(UserSyncBetweenRepositoriesTest.class);

    private static final String HOME_PATH = "/rep:security/rep:authorizables";

    private Repository repository1;
    private JackrabbitSession adminSession1;
    private UserManager userManager1;

    private TestHomeChangeObserver observer1;
    private TestHomeChangeObserver observer2;
    private JackrabbitSession adminSession2;
    private UserManager userManager2;
    private UserSyncConsumer consumer2;

    @Before
    public void setup() throws Exception {

        List<QueueEntry> payloadItems = Collections.synchronizedList(new ArrayList<QueueEntry>());

        Jcr jcr1 = new Jcr();

        UserDiffPayloadQueue queue1 = new DefaultUserDiffPayloadQueue("repo1", payloadItems);
        observer1 = new TestHomeChangeObserver("repo1", queue1);
        BackgroundObserver backgroundObserver1 = new BackgroundObserver(
                observer1,
                Executors.newSingleThreadExecutor()
        );
        jcr1.with(backgroundObserver1);


        repository1 = jcr1.createRepository();
        
        // Get admin session using default admin credentials
        adminSession1 = (JackrabbitSession) repository1.login(
            new SimpleCredentials("admin", "admin".toCharArray())
        );
        
        // Get UserManager from admin session
        userManager1 = adminSession1.getUserManager();


        // Now we instantiate the second repository
        Jcr jcr2 = new Jcr();

        UserDiffPayloadQueue queue2 = new DefaultUserDiffPayloadQueue("repo2", payloadItems);
        observer2 = new TestHomeChangeObserver("repo2", queue2);
        BackgroundObserver backgroundObserver2 = new BackgroundObserver(
                observer2,
                Executors.newSingleThreadExecutor()
        );
        jcr2.with(backgroundObserver2);

        // Create the second repository
        Repository repository2 = jcr2.createRepository();

        adminSession2 = (JackrabbitSession) repository2.login(
            new SimpleCredentials("admin", "admin".toCharArray())
        );

        userManager2 = adminSession2.getUserManager();

        consumer2 = new UserSyncConsumer("repo2", adminSession2, queue2, new MemoryBlobStore());
    }

    private static class TestHomeChangeObserver extends UserSyncInitiator {
        private String lastChange = "";

        public TestHomeChangeObserver(String instanceId, UserDiffPayloadQueue userDiffPayloadQueue) {
            super(instanceId, userDiffPayloadQueue, HOME_PATH);
        }

        @Override
        protected void enqueueChange(String change) {
            LOG.info("Change detected: {}", change);
            this.lastChange = change;
            super.enqueueChange(change);
        }

        String getLastChange() {
            return lastChange;
        }
    }

    @Test
    public void testAddingGroup() throws Exception {
        // Create a test group in the first repository
        Group testGroup = userManager1.createGroup("testGroup");
        adminSession1.save();
        TimeUnit.MILLISECONDS.sleep(500);
        System.out.println("Test group \n: " + observer1.getLastChange());

        assertNotNull(testGroup);
        assertEquals("testGroup", testGroup.getID());

        // Verify that the change is captured by the observer
        assertFalse("Should have captured changes", observer1.getLastChange().isEmpty());
        assertTrue("Should contain group addition",
                observer1.getLastChange().contains("\"rep:authorizableId\":\"testGroup\""));

        // Verify that the change is processed by the consumer
        assertNotNull(consumer2.getLastProcessedChange());
        Group testGroup2 = (Group) userManager2.getAuthorizable("testGroup");
        assertNotNull(testGroup2);
        assertEquals("testGroup", testGroup2.getID());
    }

    @Test
    public void testCreateUser() throws Exception {
        // Create a test user in the first repository
        User testUser = userManager1.createUser("testUser", "testUser");
        testUser.setProperty("email", adminSession1.getValueFactory().createValue("test@test.org"));
        adminSession1.save();
        TimeUnit.MILLISECONDS.sleep(500);

        // Verify that the user is created in the second repository
        User testUserInRepo2 = (User) userManager2.getAuthorizable("testUser");
        assertNotNull("User should be created in the second repository", testUserInRepo2);
        assertEquals("testUser", testUserInRepo2.getID());
        assertEquals("test@test.org", testUserInRepo2.getProperty("email")[0].getString());
    }

    @Test
    public void testCreateUserAndAddToGroup() throws Exception {
        // Create a test group in the first repository
        Group testGroup = userManager1.createGroup("testGroup");
        adminSession1.save();

        // Create a test user in the first repository
        User testUser = userManager1.createUser("testUser", "testUser");
        testUser.setProperty("email", adminSession1.getValueFactory().createValue("test@test.org"));
        testGroup.addMember(testUser);
        adminSession1.save();

        TimeUnit.MILLISECONDS.sleep(500);
        System.out.println("Test group \n: " + observer1.getLastChange());

        // Verify that the user is created in the second repository
        User testUserInRepo2 = (User) userManager2.getAuthorizable("testUser");
        assertNotNull("User should be created in the second repository", testUserInRepo2);
        assertEquals("testUser", testUserInRepo2.getID());
        Group testGroupInRepo2 = (Group) userManager2.getAuthorizable("testGroup");
        assertNotNull("Group should be created in the second repository", testGroupInRepo2);
        assertTrue("User should be a member of the group in the second repository",
                testGroupInRepo2.isMember(testUserInRepo2));
    }

    @Test
    public void testJsonDiffHasOnlyTheLastAddedUser() throws Exception {

        Group testGroup = userManager1.createGroup("testGroup");
        User testUser1 = userManager1.createUser("testUser1", "testUser1");
        testGroup.addMember(testUser1);
        adminSession1.save();

        // Create another user
        User testUser2 = userManager1.createUser("testUser2", "testUser2");
        testGroup.addMember(testUser2);
        adminSession1.save();
        TimeUnit.MILLISECONDS.sleep(500);

        // Verify that the last change is captured

        String lastChange = observer1.getLastChange();
        LOG.info("Last change: {}", lastChange);
        assertTrue(lastChange.contains("+testUser2"));
        assertFalse(lastChange.contains("testUser1"));
    }

    @Test
    public void testRemoveUser() throws Exception {

        Group testGroup = userManager1.createGroup("testGroup");
        adminSession1.save();

        LOG.info("Adding user {} to group {}", "testUser", "testGroup");
        User testUser = userManager1.createUser("testUser", "testUser");
        testGroup.addMember(testUser);

        LOG.info("Adding user {} to group {}", "testUser2", "testGroup");
        User testUser2 = userManager1.createUser("testUser2", "testUser2");
        testGroup.addMember(testUser2);
        adminSession1.save();
        TimeUnit.MILLISECONDS.sleep(500);

        Group testGroupInRepo2 = (Group) userManager2.getAuthorizable("testGroup");
        assertNotNull(testGroupInRepo2);

        User testUserInRepo2 = (User) userManager2.getAuthorizable("testUser");
        assertNotNull(testUserInRepo2);
        assertTrue(testGroupInRepo2.isMember(testUserInRepo2));
        User testUser2InRepo2 = (User) userManager2.getAuthorizable("testUser2");
        assertNotNull(testUser2InRepo2);
        assertTrue(testGroupInRepo2.isMember(testUser2InRepo2));

        // Remove the user from the first repository
        // iterate over all members and log
        Iterator<Authorizable> members = testGroup.getMembers();
        LOG.info("Current members:");
        while (members.hasNext()) {
            LOG.info("Member: {}", members.next().getID());
        }
        LOG.info("Removing user {} from group {}", "testUser", "testGroup");
        testGroup.removeMember(testUser);
        adminSession1.save();
        TimeUnit.MILLISECONDS.sleep(500);

        // Verify that the user is removed in the second repository
        assertFalse(testGroupInRepo2.isMember(testUserInRepo2));
        // other user should still be a member
        assertTrue(testGroupInRepo2.isMember(testUser2InRepo2));
    }

    @Test
    public void testAddGroupAndTwoUsersAtOnce() throws Exception {
    //TODO
    }
}