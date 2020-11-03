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
package org.apache.pulsar.functions.instance;

<<<<<<< HEAD
import lombok.*;
=======
import lombok.Data;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * This is the Java Instance. This is started by the runtimeSpawner using the JavaInstanceClient
 * program if invoking via a process based invocation or using JavaInstance using a thread
 * based invocation.
 */
@Data
<<<<<<< HEAD
@Setter
@Getter
@EqualsAndHashCode
@ToString
=======
>>>>>>> f773c602c... Test pr 10 (#27)
public class JavaExecutionResult {
    private Exception userException;
    private Exception systemException;
    private Object result;

    public void reset() {
        setUserException(null);
        setSystemException(null);
        setResult(null);
    }
}