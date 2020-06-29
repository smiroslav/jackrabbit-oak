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
package org.apache.jackrabbit.oak.store.remote.store.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.apache.jackrabbit.oak.store.remote.store.AbstractStoreTest;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

public class DynamoStoreTest extends AbstractStoreTest {

    private static DynamoDB client;

    @BeforeClass
    public static void setUpClass() throws Exception {
        AmazonDynamoDB api;

        try {
            api = AmazonDynamoDBClientBuilder.defaultClient();
        } catch (Exception e) {
            api = null;
        }

        Assume.assumeNotNull(api);
        client = new DynamoDB(api);
        DynamoSchema.createSchema(client, new DynamoSchema.Options());
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (client != null) {
            DynamoSchema.dropSchema(client);
            client.shutdown();
            client = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        store = new DynamoStore(client);
    }

}
