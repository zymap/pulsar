/**
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
package org.apache.pulsar.common.policies.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

<<<<<<< HEAD
public class BookiesRackConfiguration extends TreeMap<String, Map<String, BookieInfo>> {

    public boolean removeBookie(String address) {
        for (Map<String, BookieInfo> m : values()) {
            if (m.remove(address) != null ) {
                return true;
            }
        }

        return false;
    }

    public Optional<BookieInfo> getBookie(String address) {
=======
/**
 * The rack configuration map for bookies.
 */
public class BookiesRackConfiguration extends TreeMap<String, Map<String, BookieInfo>> {

    private static final long serialVersionUID = 0L;

    public synchronized boolean removeBookie(String address) {
        for (Map.Entry<String, Map<String, BookieInfo>> entry : entrySet()) {
            if (entry.getValue().remove(address) != null) {
                if (entry.getValue().isEmpty()) {
                    remove(entry.getKey());
                }
                return true;
            }
        }
        return false;
    }

    public synchronized Optional<BookieInfo> getBookie(String address) {
>>>>>>> f773c602c... Test pr 10 (#27)
        for (Map<String, BookieInfo> m : values()) {
            BookieInfo bi = m.get(address);
            if (bi != null) {
                return Optional.of(bi);
            }
        }
        return Optional.empty();
    }

<<<<<<< HEAD
    public void updateBookie(String group, String address, BookieInfo bookieInfo) {
=======
    public synchronized void updateBookie(String group, String address, BookieInfo bookieInfo) {
>>>>>>> f773c602c... Test pr 10 (#27)
        checkNotNull(group);
        checkNotNull(address);
        checkNotNull(bookieInfo);

        // Remove from any group first
        removeBookie(address);
        computeIfAbsent(group, key -> new TreeMap<>()).put(address, bookieInfo);
    }
<<<<<<< HEAD
}
=======
}
>>>>>>> f773c602c... Test pr 10 (#27)
