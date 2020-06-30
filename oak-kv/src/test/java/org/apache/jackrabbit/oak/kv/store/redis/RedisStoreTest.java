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
package org.apache.jackrabbit.oak.kv.store.redis;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.jackrabbit.oak.kv.store.AbstractStoreTest;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Value;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

public class RedisStoreTest extends AbstractStoreTest {

    private static JedisPool jedisPool;

    @BeforeClass
    public static void setUpClass() {
        try {
            jedisPool = new JedisPool(System.getProperty("redis.host"), Integer.getInteger("redis.port"));
        } catch (Exception e) {
            jedisPool = null;
        }
        Assume.assumeNotNull(jedisPool);
    }

    @AfterClass
    public static void tearDownClass() {
        if (jedisPool != null) {
            jedisPool.close();
            jedisPool = null;
        }
    }

    @Before
    public void setup() {
        store = new RedisStore(jedisPool);
    }

    @Test
    public void testManyProperties() throws Exception {
        Map<String, Value> properties = new HashMap<>();

        for (int i = 0; i < 123; i++) { // not a batch size multiplier
            properties.put("key" + i, Value.newStringValue(UUID.randomUUID().toString()));
        }

        Node node = store.getNode(putNode(properties, emptyMap()));
        assertEquals(properties, node.getProperties());
    }

    @Test
    public void testManyChildren() throws Exception {
        Map<String, ID> children = new HashMap<>();

        for (int i = 0; i < 123; i++) { // not a batch size multiplier
            children.put("child" + i, putNode(emptyMap(), emptyMap()));
        }

        Node node = store.getNode(putNode(emptyMap(), children));
        assertEquals(children, node.getChildren());
    }

    @Test
    public void testLongProperty() throws Exception {
        Map<String, Value> properties = new HashMap<>();
        List<String> property = new ArrayList<>();
        for (int i = 0; i < 123; i++) {
            property.add("value" + i);
        }
        properties.put("prop", Value.newStringArray(property));

        Node node = store.getNode(putNode(properties, emptyMap()));
        assertEquals(properties, node.getProperties());
    }

    private ID putNode(Map<String, Value> properties, Map<String, ID> children) throws IOException {
        return store.putNode(
            store.putProperties(properties),
            store.putChildren(children)
        );
    }

}
