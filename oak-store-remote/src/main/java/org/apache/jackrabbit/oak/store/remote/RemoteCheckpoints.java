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

package org.apache.jackrabbit.oak.store.remote;

import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.store.remote.store.Storage;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class RemoteCheckpoints {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Storage storage;

    private final BlobStore blobStore;

    RemoteCheckpoints(Storage storage, BlobStore blobStore) {
        this.storage = storage;
        this.blobStore = blobStore;
    }

    Map<String, String> checkpointInfo(String reference) throws IOException {
        return null;
    }

    Iterable<String> checkpoints() throws IOException {
       return null;
    }

    NodeState retrieve(String reference) throws IOException {
        return null;
    }

    boolean release(String reference) throws IOException {

        return true;
    }

    Iterable<RemoteCheckpoint> getCheckpoints() throws IOException {

        return null;
    }



}
