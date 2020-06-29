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

package org.apache.jackrabbit.oak.store.remote.osgi;

import java.io.File;
import java.util.Map;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.jackrabbit.oak.store.remote.osgi.LevelKVNodeStoreService.Configuration;
import org.apache.jackrabbit.oak.store.remote.store.ID;
import org.apache.jackrabbit.oak.store.remote.store.Node;
import org.apache.jackrabbit.oak.store.remote.store.Store;
import org.apache.jackrabbit.oak.store.remote.store.Value;
import org.apache.jackrabbit.oak.store.remote.store.cache.CachedStore;
import org.apache.jackrabbit.oak.store.remote.store.level.LevelStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = Configuration.class)
public class LevelKVNodeStoreService {

    @ObjectClassDefinition
    public @interface Configuration {

        @AttributeDefinition(name = "Path")
        String path();

        @AttributeDefinition(name = "Node cache size")
        long nodeCacheSize() default 50000;

        @AttributeDefinition(name = "Properties cache size")
        long propertiesCacheSize() default 50000;

        @AttributeDefinition(name = "Children cache size")
        long childrenCacheSize() default 50000;

    }

    @Reference
    private BlobStore blobStore;

    private DB db;

    @Activate
    public void activate(BundleContext ctx, Configuration cfg) throws Exception {
        db = JniDBFactory.factory.open(new File(cfg.path()), newOptions());
        Store store = new CachedStore(
            new LevelStore(db),
            newNodeCache(cfg),
            newPropertiesCache(cfg),
            newChildrenCache(cfg)
        );
        KVNodeStoreRegistration.registerKVNodeStore(ctx, store, blobStore);
    }

    @Deactivate
    public void deactivate() throws Exception {
        db.close();
        db = null;
    }

    private Options newOptions() {
        Options options = new Options();
        options.createIfMissing(true);
        return options;
    }

    private Cache<ID, Node> newNodeCache(Configuration cfg) {
        return CacheBuilder.newBuilder()
            .maximumSize(cfg.nodeCacheSize())
            .build();
    }

    private Cache<ID, Map<String, Value>> newPropertiesCache(Configuration cfg) {
        return CacheBuilder.newBuilder()
            .maximumSize(cfg.propertiesCacheSize())
            .build();
    }

    private Cache<ID, Map<String, ID>> newChildrenCache(Configuration cfg) {
        return CacheBuilder.newBuilder()
            .maximumSize(cfg.childrenCacheSize())
            .build();
    }

}
