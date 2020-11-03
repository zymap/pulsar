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
package org.apache.pulsar.broker.namespace;

<<<<<<< HEAD
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
=======
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceBundle;
<<<<<<< HEAD
=======
import org.apache.pulsar.common.util.FutureUtil;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnedBundle {
    private static final Logger LOG = LoggerFactory.getLogger(OwnedBundle.class);

    private final NamespaceBundle bundle;

    /**
     * {@link #nsLock} is used to protect read/write access to {@link #active} flag and the corresponding code section
     * based on {@link #active} flag
     */
    private final ReentrantReadWriteLock nsLock = new ReentrantReadWriteLock();
    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<OwnedBundle> IS_ACTIVE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(OwnedBundle.class, "isActive");
    private volatile int isActive = TRUE;

    /**
     * constructor
     *
<<<<<<< HEAD
     * @param nsname
=======
     * @param suName
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    public OwnedBundle(NamespaceBundle suName) {
        this.bundle = suName;
        IS_ACTIVE_UPDATER.set(this, TRUE);
    };

    /**
     * Constructor to allow set initial active flag
     *
<<<<<<< HEAD
     * @param nsname
     * @param nssvc
=======
     * @param suName
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param active
     */
    public OwnedBundle(NamespaceBundle suName, boolean active) {
        this.bundle = suName;
        IS_ACTIVE_UPDATER.set(this, active ? TRUE : FALSE);
    }

    /**
     * Access to the namespace name
     *
     * @return NamespaceName
     */
    public NamespaceBundle getNamespaceBundle() {
        return this.bundle;
    }

    /**
     * It unloads the bundle by closing all topics concurrently under this bundle.
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * <pre>
     * a. disable bundle ownership in memory and not in zk
     * b. close all the topics concurrently
     * c. delete ownership znode from zookeeper.
     * </pre>
<<<<<<< HEAD
     * 
     * @param pulsar
     * @param timeout
     *            timeout for unloading bundle. It doesn't throw exception if it timesout while waiting on closing all
=======
     *
     * @param pulsar
     * @param timeout
     *            timeout for unloading bundle. It doesn't throw exception if it times out while waiting on closing all
>>>>>>> f773c602c... Test pr 10 (#27)
     *            topics
     * @param timeoutUnit
     * @throws Exception
     */
<<<<<<< HEAD
    public void handleUnloadRequest(PulsarService pulsar, long timeout, TimeUnit timeoutUnit) throws Exception {

        long unloadBundleStartTime = System.nanoTime();
        // Need a per namespace RenetrantReadWriteLock
        // Here to do a writeLock to set the flag and proceed to check and close connections
        while (!this.nsLock.writeLock().tryLock(1, TimeUnit.SECONDS)) {
            // Using tryLock to avoid deadlocks caused by 2 threads trying to acquire 2 readlocks (eg: JMS replicators)
            // while a handleUnloadRequest happens in the middle
            LOG.warn("Contention on OwnedBundle rw lock. Retrying to acquire lock write lock");
        }

        try {
            // set the flag locally s.t. no more producer/consumer to this namespace is allowed
            if (!IS_ACTIVE_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                // An exception is thrown when the namespace is not in active state (i.e. another thread is
                // removing/have removed it)
                throw new IllegalStateException(
                        "Namespace is not active. ns:" + this.bundle + "; state:" + IS_ACTIVE_UPDATER.get(this));
            }
        } finally {
            // no matter success or not, unlock
            this.nsLock.writeLock().unlock();
        }

        int unloadedTopics = 0;
        try {
            LOG.info("Disabling ownership: {}", this.bundle);
            pulsar.getNamespaceService().getOwnershipCache().updateBundleState(this.bundle, false);

            // close topics forcefully
            try {
                unloadedTopics = pulsar.getBrokerService().unloadServiceUnit(bundle).get(timeout, timeoutUnit);
            } catch (TimeoutException e) {
                // ignore topic-close failure to unload bundle
                LOG.error("Failed to close topics in namespace {} in {}/{} timeout", bundle.toString(), timeout,
                        timeoutUnit);
            } catch (Exception e) {
                // ignore topic-close failure to unload bundle
                LOG.error("Failed to close topics under namespace {}", bundle.toString(), e);
            }
            // delete ownership node on zk
            try {
                pulsar.getNamespaceService().getOwnershipCache().removeOwnership(bundle).get(timeout, timeoutUnit);
            } catch (Exception e) {
                // Failed to remove ownership node: enable namespace-bundle again so, it can serve new topics
                pulsar.getNamespaceService().getOwnershipCache().updateBundleState(this.bundle, true);
                throw new RuntimeException(String.format("Failed to delete ownership node %s", bundle.toString()),
                        e.getCause());
            }
        } catch (Exception e) {
            LOG.error("Failed to unload a namespace {}", bundle.toString(), e);
            throw new RuntimeException(e);
        }

        double unloadBundleTime = TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - unloadBundleStartTime));
        LOG.info("Unloading {} namespace-bundle with {} topics completed in {} ms", this.bundle, unloadedTopics, unloadBundleTime);
=======
    public CompletableFuture<Void> handleUnloadRequest(PulsarService pulsar, long timeout, TimeUnit timeoutUnit) {
        long unloadBundleStartTime = System.nanoTime();
        // Need a per namespace RenetrantReadWriteLock
        // Here to do a writeLock to set the flag and proceed to check and close connections
        try {
            while (!this.nsLock.writeLock().tryLock(1, TimeUnit.SECONDS)) {
                // Using tryLock to avoid deadlocks caused by 2 threads trying to acquire 2 readlocks (eg: replicators)
                // while a handleUnloadRequest happens in the middle
                LOG.warn("Contention on OwnedBundle rw lock. Retrying to acquire lock write lock");
            }

            try {
                // set the flag locally s.t. no more producer/consumer to this namespace is allowed
                if (!IS_ACTIVE_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                    // An exception is thrown when the namespace is not in active state (i.e. another thread is
                    // removing/have removed it)
                    return FutureUtil.failedFuture(new IllegalStateException(
                            "Namespace is not active. ns:" + this.bundle + "; state:" + IS_ACTIVE_UPDATER.get(this)));
                }
            } finally {
                // no matter success or not, unlock
                this.nsLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            return FutureUtil.failedFuture(e);
        }

        AtomicInteger unloadedTopics = new AtomicInteger();
        LOG.info("Disabling ownership: {}", this.bundle);

        // close topics forcefully
        return pulsar.getNamespaceService().getOwnershipCache()
                .updateBundleState(this.bundle, false)
                .thenCompose(v -> pulsar.getBrokerService().unloadServiceUnit(bundle, true, timeout, timeoutUnit))
                .handle((numUnloadedTopics, ex) -> {
                    if (ex != null) {
                        // ignore topic-close failure to unload bundle
                        LOG.error("Failed to close topics under namespace {}", bundle.toString(), ex);
                    } else {
                        unloadedTopics.set(numUnloadedTopics);
                    }
                    return null;
                })
                .thenCompose(v -> {
                    // delete ownership node on zk
                    return pulsar.getNamespaceService().getOwnershipCache().removeOwnership(bundle);
                }).whenComplete((ignored, ex) -> {
                    double unloadBundleTime = TimeUnit.NANOSECONDS
                            .toMillis((System.nanoTime() - unloadBundleStartTime));
                    LOG.info("Unloading {} namespace-bundle with {} topics completed in {} ms", this.bundle,
                            unloadedTopics, unloadBundleTime, ex);
                });
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    /**
     * Access method to the namespace state to check whether the namespace is active or not
     *
     * @return boolean value indicate that the namespace is active or not.
     */
    public boolean isActive() {
        return IS_ACTIVE_UPDATER.get(this) == TRUE;
    }

    public void setActive(boolean active) {
        IS_ACTIVE_UPDATER.set(this, active ? TRUE : FALSE);
    }
}
