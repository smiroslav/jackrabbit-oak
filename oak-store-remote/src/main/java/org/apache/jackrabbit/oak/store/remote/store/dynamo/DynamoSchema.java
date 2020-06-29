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

import static java.util.Collections.singletonList;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class DynamoSchema {

    public static class Options {

        private long nodesReadCapacityUnits = 10L;

        private long nodesWriteCapacityUnits = 10L;

        private long tagsReadCapacityUnits = 10L;

        private long tagsWriteCapacityUnits = 10L;

        private long propertiesReadCapacityUnits = 10L;

        private long propertiesWriteCapacityUnits = 10L;

        private long childrenReadCapacityUnits = 10L;

        private long childrenWriteCapacityUnits = 10L;

        public long getNodesReadCapacityUnits() {
            return nodesReadCapacityUnits;
        }

        public Options setNodesReadCapacityUnits(long nodesReadCapacityUnits) {
            this.nodesReadCapacityUnits = nodesReadCapacityUnits;
            return this;
        }

        public long getNodesWriteCapacityUnits() {
            return nodesWriteCapacityUnits;
        }

        public Options setNodesWriteCapacityUnits(long nodesWriteCapacityUnits) {
            this.nodesWriteCapacityUnits = nodesWriteCapacityUnits;
            return this;
        }

        public long getTagsReadCapacityUnits() {
            return tagsReadCapacityUnits;
        }

        public Options setTagsReadCapacityUnits(long tagsReadCapacityUnits) {
            this.tagsReadCapacityUnits = tagsReadCapacityUnits;
            return this;
        }

        public long getTagsWriteCapacityUnits() {
            return tagsWriteCapacityUnits;
        }

        public Options setTagsWriteCapacityUnits(long tagsWriteCapacityUnits) {
            this.tagsWriteCapacityUnits = tagsWriteCapacityUnits;
            return this;
        }

        public long getPropertiesReadCapacityUnits() {
            return propertiesReadCapacityUnits;
        }

        public Options setPropertiesReadCapacityUnits(long propertiesReadCapacityUnits) {
            this.propertiesReadCapacityUnits = propertiesReadCapacityUnits;
            return this;
        }

        public long getPropertiesWriteCapacityUnits() {
            return propertiesWriteCapacityUnits;
        }

        public Options setPropertiesWriteCapacityUnits(long propertiesWriteCapacityUnits) {
            this.propertiesWriteCapacityUnits = propertiesWriteCapacityUnits;
            return this;
        }

        public long getChildrenReadCapacityUnits() {
            return childrenReadCapacityUnits;
        }

        public Options setChildrenReadCapacityUnits(long childrenReadCapacityUnits) {
            this.childrenReadCapacityUnits = childrenReadCapacityUnits;
            return this;
        }

        public long getChildrenWriteCapacityUnits() {
            return childrenWriteCapacityUnits;
        }

        public Options setChildrenWriteCapacityUnits(long childrenWriteCapacityUnits) {
            this.childrenWriteCapacityUnits = childrenWriteCapacityUnits;
            return this;
        }
    }

    private DynamoSchema() {
        // Prevent instantiation.
    }

    public static void createSchema(DynamoDB client, Options options) throws InterruptedException {
        createTagsTable(client, options);
        createNodesTable(client, options);
        createPropertiesTable(client, options);
        createChildrenTable(client, options);
    }

    public static void dropSchema(DynamoDB client) throws InterruptedException {
        dropTable(client, "tags");
        dropTable(client, "nodes");
        dropTable(client, "properties");
        dropTable(client, "children");
    }

    private static void createTagsTable(DynamoDB client, Options options) throws InterruptedException {
        try {
            client.getTable("tags").describe();
            return;
        } catch (ResourceNotFoundException e) {
            // Ignore, the table should not exist.
        }
        client.createTable(
            "tags",
            singletonList(new KeySchemaElement("tag", KeyType.HASH)),
            singletonList(new AttributeDefinition("tag", ScalarAttributeType.S)),
            new ProvisionedThroughput(options.getTagsReadCapacityUnits(), options.getTagsWriteCapacityUnits())
        ).waitForActive();
    }

    private static void createNodesTable(DynamoDB client, Options options) throws InterruptedException {
        try {
            client.getTable("nodes").describe();
            return;
        } catch (ResourceNotFoundException e) {
            // Ignore, the table should not exist.
        }
        client.createTable(
            "nodes",
            singletonList(new KeySchemaElement("id", KeyType.HASH)),
            singletonList(new AttributeDefinition("id", ScalarAttributeType.B)),
            new ProvisionedThroughput(options.getNodesReadCapacityUnits(), options.getNodesWriteCapacityUnits())
        ).waitForActive();
    }

    private static void createPropertiesTable(DynamoDB client, Options options) throws InterruptedException {
        try {
            client.getTable("properties").describe();
            return;
        } catch (ResourceNotFoundException e) {
            // Ignore, the table should not exist.
        }
        client.createTable(
            "properties",
            singletonList(new KeySchemaElement("id", KeyType.HASH)),
            singletonList(new AttributeDefinition("id", ScalarAttributeType.B)),
            new ProvisionedThroughput(options.getPropertiesReadCapacityUnits(), options.getPropertiesWriteCapacityUnits())
        ).waitForActive();
    }

    private static void createChildrenTable(DynamoDB client, Options options) throws InterruptedException {
        try {
            client.getTable("children").describe();
            return;
        } catch (ResourceNotFoundException e) {
            // Ignore, the table should not exist.
        }
        client.createTable(
            "children",
            singletonList(new KeySchemaElement("id", KeyType.HASH)),
            singletonList(new AttributeDefinition("id", ScalarAttributeType.B)),
            new ProvisionedThroughput(options.getChildrenReadCapacityUnits(), options.getChildrenWriteCapacityUnits())
        ).waitForActive();
    }

    private static void dropTable(DynamoDB client, String name) throws InterruptedException {
        try {
            Table table = client.getTable(name);
            table.delete();
            table.waitForDelete();
        } catch (ResourceNotFoundException e) {
            // Ignore, the table might not exist.
        }
    }

}
