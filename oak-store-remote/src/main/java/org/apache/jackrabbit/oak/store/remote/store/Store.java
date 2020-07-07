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

package org.apache.jackrabbit.oak.store.remote.store;

import java.io.IOException;
import java.util.Map;

public interface Store {

    ID getTag(String tag) throws IOException;

    void putTag(String tag, ID id) throws IOException;

    void deleteTag(String tag) throws IOException;

    NodeDel getNode(ID id) throws IOException;

    ID putNode(ID properties, ID children) throws IOException;

    Map<String, Value> getProperties(ID id) throws IOException;

    ID putProperties(Map<String, Value> properties) throws IOException;

    Map<String, ID> getChildren(ID id) throws IOException;

    ID putChildren(Map<String, ID> children) throws IOException;

}
