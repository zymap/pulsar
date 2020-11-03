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
package org.apache.pulsar.client.api;

/**
 * Standard hashing functions available when choosing the partition to use for a particular message.
 */
public enum HashingScheme {

    /**
<<<<<<< HEAD
     * Use regular <code>String.hashCode()</code>
=======
     * Use regular <code>String.hashCode()</code>.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    JavaStringHash,

    /**
     * Use Murmur3 hashing function.
     * <a href="https://en.wikipedia.org/wiki/MurmurHash">https://en.wikipedia.org/wiki/MurmurHash</a>
     */
    Murmur3_32Hash

}
