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

package org.apache.jackrabbit.oak.kv.store.level;

import org.apache.jackrabbit.oak.kv.store.AbstractStoreTest;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class LevelStoreTest extends AbstractStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DB db;

    @Before
    public void setUp() throws Exception {
        Options options = new Options();
        options.createIfMissing(true);
        db = JniDBFactory.factory.open(folder.getRoot(), options);
        store = new LevelStore(db);
    }

    @After
    public void tearDown() throws Exception {
        db.close();
    }

}
