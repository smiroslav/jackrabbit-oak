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

package org.apache.jackrabbit.oak.store.remote.store.osgi;

import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.store.remote.RemoteNodeStore;
import org.apache.jackrabbit.oak.store.remote.store.Storage;
import org.osgi.framework.BundleContext;

import java.util.Collections;

class NodeStoreRegistration {

    private NodeStoreRegistration() {
        // Prevent instantiation.
    }

    static void registerKVNodeStore(BundleContext ctx, Storage storage, BlobStore blobStore) {
        RemoteNodeStore remoteNodeStore = new RemoteNodeStore(storage, blobStore);

        ObserverTracker observerTracker = new ObserverTracker(remoteNodeStore);
        observerTracker.start(ctx);

        Whiteboard whiteboard = new OsgiWhiteboard(ctx);
        whiteboard.register(NodeStore.class, remoteNodeStore, Collections.emptyMap());
        //whiteboard.register(Descriptors.class, new KVDiscoveryLiteDescriptors(nodeStore), Collections.emptyMap());
        //registerMBean(whiteboard, CheckpointMBean.class, new KVCheckpointMBean(nodeStore), CheckpointMBean.TYPE, "Oak KV Checkpoints");
    }

}
