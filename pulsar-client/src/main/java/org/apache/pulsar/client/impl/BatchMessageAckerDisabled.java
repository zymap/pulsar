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
package org.apache.pulsar.client.impl;

<<<<<<< HEAD
=======
import java.util.BitSet;

>>>>>>> f773c602c... Test pr 10 (#27)
class BatchMessageAckerDisabled extends BatchMessageAcker {

    static final BatchMessageAckerDisabled INSTANCE = new BatchMessageAckerDisabled();

    private BatchMessageAckerDisabled() {
<<<<<<< HEAD
        super(null, 0);
=======
        super(new BitSet(), 0);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public synchronized int getBatchSize() {
        return 0;
    }

    @Override
    public boolean ackIndividual(int batchIndex) {
        return true;
    }

    @Override
    public boolean ackCumulative(int batchIndex) {
        return true;
    }

    @Override
    public int getOutstandingAcks() {
        return 0;
    }
}
