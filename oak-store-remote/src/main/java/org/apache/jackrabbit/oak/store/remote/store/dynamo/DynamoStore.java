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

import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jackrabbit.oak.store.remote.io.KryoConverter;
import org.apache.jackrabbit.oak.store.remote.store.ID;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.Store;
import org.apache.jackrabbit.oak.store.remote.store.Value;

public class DynamoStore implements Store {

    private static final KryoConverter converter = new KryoConverter() {

        @Override
        protected ID readID(Input input) {
            long msb = input.readLong();
            long lsb = input.readLong();
            return new DynamoID(new UUID(msb, lsb));
        }

        @Override
        protected void writeID(Output output, ID id) {
            if (id instanceof DynamoID) {
                UUID uuid = ((DynamoID) id).getID();
                output.writeLong(uuid.getMostSignificantBits());
                output.writeLong(uuid.getLeastSignificantBits());
            } else {
                throw new IllegalArgumentException("id");
            }
        }

        @Override
        protected Node createNode(ID properties, ID children) {
            return new DynamoNode(properties, children);
        }

    };

    private final DynamoDB client;

    public DynamoStore(DynamoDB client) {
        this.client = client;
    }

    private Table tags() {
        return client.getTable("tags");
    }

    private Table nodes() {
        return client.getTable("nodes");
    }

    private Table properties() {
        return client.getTable("properties");
    }

    private Table children() {
        return client.getTable("children");
    }

    @Override
    public ID getTag(String tag) {
        Item item = tags().getItem("tag", tag);
        if (item == null) {
            return null;
        }
        return converter.readID(item.getBinary("id"));
    }

    @Override
    public void putTag(String tag, ID id) {
        tags().putItem(new Item()
            .withString("tag", tag)
            .withBinary("id", converter.writeID(id)));
    }

    @Override
    public void deleteTag(String tag) {
        tags().deleteItem("tag", tag);
    }

    @Override
    public Node getNode(ID id) {
        Item item = nodes().getItem("id", converter.writeID(id));
        if (item == null) {
            return null;
        }
        return converter.readNode(item.getBinary("data"));
    }

    @Override
    public ID putNode(ID properties, ID children) {
        ID id = new DynamoID(UUID.randomUUID());
        nodes().putItem(new Item()
            .withBinary("id", converter.writeID(id))
            .withBinary("data", converter.writeNode(properties, children)));
        return id;
    }

    @Override
    public Map<String, Value> getProperties(ID id) {
        Item item = properties().getItem("id", converter.writeID(id));
        if (item == null) {
            return null;
        }
        return converter.readProperties(item.getBinary("data"));
    }

    @Override
    public ID putProperties(Map<String, Value> properties) {
        ID id = new DynamoID(UUID.randomUUID());
        properties().putItem(new Item()
            .withBinary("id", converter.writeID(id))
            .withBinary("data", converter.writeProperties(properties)));
        return id;
    }

    @Override
    public Map<String, ID> getChildren(ID id) {
        Item item = children().getItem("id", converter.writeID(id));
        if (item == null) {
            return null;
        }
        return converter.readChildren(item.getBinary("data"));
    }

    @Override
    public ID putChildren(Map<String, ID> children) {
        ID id = new DynamoID(UUID.randomUUID());
        children().putItem(new Item()
            .withBinary("id", converter.writeID(id))
            .withBinary("data", converter.writeChildren(children)));
        return id;
    }

}
