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

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.google.common.base.Objects;

/**
 * Resource quota for a namespace or namespace bundle.
 */
public class ResourceQuota {

    // messages published per second
    private double msgRateIn;
    // messages consumed per second
    private double msgRateOut;
    // incoming bytes per second
    private double bandwidthIn;
    // outgoing bytes per second
    private double bandwidthOut;
    // used memory in Mbytes
    private double memory;
    // allow the quota be dynamically re-calculated according to real traffic
    private boolean dynamic;

    public ResourceQuota() {
        msgRateIn = 0.0;
        msgRateOut = 0.0;
        bandwidthIn = 0.0;
        bandwidthOut = 0.0;
        memory = 0.0;
        dynamic = true;
    }

    /**
<<<<<<< HEAD
     * Set incoming message rate quota
     * 
=======
     * Set incoming message rate quota.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param msgRateIn
     *            incoming messages rate quota (msg/sec)
     */
    public void setMsgRateIn(double msgRateIn) {
        this.msgRateIn = msgRateIn;
    }

    /**
<<<<<<< HEAD
     * Get incoming message rate quota
     * 
=======
     * Get incoming message rate quota.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return incoming message rate quota (msg/sec)
     */
    public double getMsgRateIn() {
        return this.msgRateIn;
    }

    /**
<<<<<<< HEAD
     * Set outgoing message rate quota
     * 
=======
     * Set outgoing message rate quota.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param msgRateOut
     *            outgoing messages rate quota (msg/sec)
     */
    public void setMsgRateOut(double msgRateOut) {
        this.msgRateOut = msgRateOut;
    }

    /**
<<<<<<< HEAD
     * Get outgoing message rate quota
     * 
=======
     * Get outgoing message rate quota.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return outgoing message rate quota (msg/sec)
     */
    public double getMsgRateOut() {
        return this.msgRateOut;
    }

    /**
<<<<<<< HEAD
     * Set inbound bandwidth quota
     * 
=======
     * Set inbound bandwidth quota.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param bandwidthIn
     *            inbound bandwidth quota (bytes/sec)
     */
    public void setBandwidthIn(double bandwidthIn) {
        this.bandwidthIn = bandwidthIn;
    }

    /**
<<<<<<< HEAD
     * Get inbound bandwidth quota
     * 
=======
     * Get inbound bandwidth quota.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return inbound bandwidth quota (bytes/sec)
     */
    public double getBandwidthIn() {
        return this.bandwidthIn;
    }

    /**
<<<<<<< HEAD
     * Set outbound bandwidth quota
     * 
=======
     * Set outbound bandwidth quota.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param bandwidthOut
     *            outbound bandwidth quota (bytes/sec)
     */
    public void setBandwidthOut(double bandwidthOut) {
        this.bandwidthOut = bandwidthOut;
    }

    /**
<<<<<<< HEAD
     * Get outbound bandwidth quota
     * 
=======
     * Get outbound bandwidth quota.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return outbound bandwidth quota (bytes/sec)
     */
    public double getBandwidthOut() {
        return this.bandwidthOut;
    }

    /**
<<<<<<< HEAD
     * Set memory quota
     * 
=======
     * Set memory quota.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param memory
     *            memory quota (Mbytes)
     */
    public void setMemory(double memory) {
        this.memory = memory;
    }

    /**
<<<<<<< HEAD
     * Get memory quota
     * 
=======
     * Get memory quota.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return memory quota (Mbytes)
     */
    public double getMemory() {
        return this.memory;
    }

    /**
<<<<<<< HEAD
     * Set dynamic to true/false
     * 
=======
     * Set dynamic to true/false.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param dynamic
     *            allow the quota to be dynamically re-calculated
     */
    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    /**
<<<<<<< HEAD
     * Get dynamic setting
     * 
=======
     * Get dynamic setting.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return is dynamic or not
     */
    public boolean getDynamic() {
        return this.dynamic;
    }

    /**
<<<<<<< HEAD
     * Check if this is a valid quota definition
=======
     * Check if this is a valid quota definition.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    @JsonIgnore
    public boolean isValid() {
        if (this.msgRateIn > 0.0 && this.msgRateOut > 0.0 && this.bandwidthIn > 0.0 && this.bandwidthOut > 0.0
                && this.memory > 0.0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Add quota.
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param quota
     *            <code>ResourceQuota</code> to add
     */
    public void add(ResourceQuota quota) {
        this.msgRateIn += quota.msgRateIn;
        this.msgRateOut += quota.msgRateOut;
        this.bandwidthIn += quota.bandwidthIn;
        this.bandwidthOut += quota.bandwidthOut;
        this.memory += quota.memory;
    }

    /**
     * Substract quota.
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param quota
     *            <code>ResourceQuota</code> to substract
     */
    public void substract(ResourceQuota quota) {
        this.msgRateIn -= quota.msgRateIn;
        this.msgRateOut -= quota.msgRateOut;
        this.bandwidthIn -= quota.bandwidthIn;
        this.bandwidthOut -= quota.bandwidthOut;
        this.memory -= quota.memory;
    }

    @Override
<<<<<<< HEAD
=======
    public int hashCode() {
        return Objects.hashCode(msgRateIn, msgRateOut, bandwidthIn,
                bandwidthOut, memory, dynamic);
    }

    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public boolean equals(Object obj) {
        if (obj instanceof ResourceQuota) {
            ResourceQuota other = (ResourceQuota) obj;
            return Objects.equal(this.msgRateIn, other.msgRateIn) && Objects.equal(this.msgRateOut, other.msgRateOut)
                    && Objects.equal(this.bandwidthIn, other.bandwidthIn)
                    && Objects.equal(this.bandwidthOut, other.bandwidthOut) && Objects.equal(this.memory, other.memory)
                    && Objects.equal(this.dynamic, other.dynamic);
        }
        return false;
    }
}
