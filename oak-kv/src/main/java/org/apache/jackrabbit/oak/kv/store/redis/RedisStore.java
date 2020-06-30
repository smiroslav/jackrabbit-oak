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

import static java.lang.String.format;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jackrabbit.oak.kv.io.KryoConverter;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Store;
import org.apache.jackrabbit.oak.kv.store.Value;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisStore implements Store {

    private static final KryoConverter converter = new KryoConverter() {

        @Override
        protected ID readID(Input input) {
            long msb = input.readLong();
            long lsb = input.readLong();
            return new RedisID(new UUID(msb, lsb));
        }

        @Override
        protected void writeID(Output output, ID id) {
            if (id instanceof RedisID) {
                UUID uuid = ((RedisID) id).getID();
                output.writeLong(uuid.getMostSignificantBits());
                output.writeLong(uuid.getLeastSignificantBits());
            } else {
                throw new IllegalArgumentException("id");
            }
        }

        @Override
        protected Node createNode(ID properties, ID children) {
            return new RedisNode(properties, children);
        }

    };

    private final JedisPool pool;

    public RedisStore(JedisPool pool) {
        this.pool = pool;
    }

    private static byte[] tagKey(String tag) {
        return bytes(format("t-%s", tag));
    }

    @Override
    public ID getTag(String tag) {
        try (Jedis jedis = pool.getResource()) {
            byte[] value = jedis.get(tagKey(tag));
            if (value == null) {
                return null;
            }
            return converter.readID(value);
        }
    }

    @Override
    public void putTag(String tag, ID id) {
        if (id instanceof RedisID) {
            try (Jedis jedis = pool.getResource()) {
                jedis.set(tagKey(tag), converter.writeID(id));
            }
        } else {
            throw new IllegalArgumentException("id");
        }
    }

    @Override
    public void deleteTag(String tag) {
        try (Jedis jedis = pool.getResource()) {
            jedis.del(tagKey(tag));
        }
    }

    @Override
    public Node getNode(ID id) {
        if (id instanceof RedisID) {
            return getNode((RedisID) id);
        }
        throw new IllegalArgumentException("id");
    }

    private Node getNode(RedisID id) {
        try (Jedis jedis = pool.getResource()) {
            byte[] value = jedis.get(converter.writeID(id));
            if (value == null) {
                return null;
            }
            return converter.readNode(value);
        }
    }

    @Override
    public ID putNode(ID properties, ID children) {
        RedisID id = new RedisID(UUID.randomUUID());
        try (Jedis jedis = pool.getResource()) {
            jedis.set(converter.writeID(id), converter.writeNode(properties, children));
        }
        return id;
    }

    @Override
    public Map<String, Value> getProperties(ID id) throws IOException {
        if (id instanceof RedisID) {
            return getProperties((RedisID) id);
        }
        throw new IllegalArgumentException("id");
    }

    private Map<String, Value> getProperties(RedisID id) {
        try (Jedis jedis = pool.getResource()) {
            byte[] value = jedis.get(converter.writeID(id));
            if (value == null) {
                return null;
            }
            return converter.readProperties(value);
        }
    }

    @Override
    public ID putProperties(Map<String, Value> properties) throws IOException {
        RedisID id = new RedisID(UUID.randomUUID());
        try (Jedis jedis = pool.getResource()) {
            jedis.set(converter.writeID(id), converter.writeProperties(properties));
        }
        return id;
    }

    @Override
    public Map<String, ID> getChildren(ID id) throws IOException {
        if (id instanceof RedisID) {
            return getChildren((RedisID) id);
        }
        throw new IllegalArgumentException("id");
    }

    private Map<String, ID> getChildren(RedisID id) {
        try (Jedis jedis = pool.getResource()) {
            byte[] value = jedis.get(converter.writeID(id));
            if (value == null) {
                return null;
            }
            return converter.readChildren(value);
        }
    }

    @Override
    public ID putChildren(Map<String, ID> children) throws IOException {
        RedisID id = new RedisID(UUID.randomUUID());
        try (Jedis jedis = pool.getResource()) {
            jedis.set(converter.writeID(id), converter.writeChildren(children));
        }
        return id;
    }

}
