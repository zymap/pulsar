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
package org.apache.pulsar.functions.worker.scheduler;

<<<<<<< HEAD
=======
import lombok.extern.slf4j.Slf4j;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.Function.Instance;

import com.google.common.collect.Lists;

<<<<<<< HEAD
=======
import java.util.Comparator;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD
import java.util.Set;

=======
import java.util.Queue;
import java.util.Set;

@Slf4j
>>>>>>> f773c602c... Test pr 10 (#27)
public class RoundRobinScheduler implements IScheduler {

    @Override
    public List<Assignment> schedule(List<Instance> unassignedFunctionInstances,
<<<<<<< HEAD
            List<Assignment> currentAssignments, Set<String> workers) {

        Map<String, List<Assignment>> workerIdToAssignment = new HashMap<>();
=======
                                     List<Assignment> currentAssignments, Set<String> workers) {

        Map<String, List<Instance>> workerIdToAssignment = new HashMap<>();
>>>>>>> f773c602c... Test pr 10 (#27)
        List<Assignment> newAssignments = Lists.newArrayList();

        for (String workerId : workers) {
            workerIdToAssignment.put(workerId, new LinkedList<>());
        }

        for (Assignment existingAssignment : currentAssignments) {
<<<<<<< HEAD
            workerIdToAssignment.get(existingAssignment.getWorkerId()).add(existingAssignment);
=======
            workerIdToAssignment.get(existingAssignment.getWorkerId()).add(existingAssignment.getInstance());
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        for (Instance unassignedFunctionInstance : unassignedFunctionInstances) {
            String workerId = findNextWorker(workerIdToAssignment);
            Assignment newAssignment = Assignment.newBuilder().setInstance(unassignedFunctionInstance)
                    .setWorkerId(workerId).build();
<<<<<<< HEAD
            workerIdToAssignment.get(workerId).add(newAssignment);
=======
            workerIdToAssignment.get(workerId).add(newAssignment.getInstance());
>>>>>>> f773c602c... Test pr 10 (#27)
            newAssignments.add(newAssignment);
        }

        return newAssignments;
    }

<<<<<<< HEAD
    private String findNextWorker(Map<String, List<Assignment>> workerIdToAssignment) {
        String targetWorkerId = null;
        int least = Integer.MAX_VALUE;
        for (Map.Entry<String, List<Assignment>> entry : workerIdToAssignment.entrySet()) {
            String workerId = entry.getKey();
            List<Assignment> workerAssigments = entry.getValue();
            if (workerAssigments.size() < least) {
                targetWorkerId = workerId;
                least = workerAssigments.size();
=======
    private String findNextWorker(Map<String, List<Instance>> workerIdToAssignment) {
        String targetWorkerId = null;
        int least = Integer.MAX_VALUE;
        for (Map.Entry<String, List<Instance>> entry : workerIdToAssignment.entrySet()) {
            String workerId = entry.getKey();
            List<Instance> workerAssignments = entry.getValue();
            if (workerAssignments.size() < least) {
                targetWorkerId = workerId;
                least = workerAssignments.size();
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        }
        return targetWorkerId;
    }
<<<<<<< HEAD
=======

    @Override
    public List<Assignment> rebalance(List<Assignment> currentAssignments, Set<String> workers) {

        Map<String, List<Instance>> workerToAssignmentMap = new HashMap<>();

        workers.forEach(workerId -> workerToAssignmentMap.put(workerId, new LinkedList<>()));

        currentAssignments.forEach(assignment -> workerToAssignmentMap.computeIfAbsent(assignment.getWorkerId(), s -> new LinkedList<>()).add(assignment.getInstance()));

        List<Assignment> newAssignments = new LinkedList<>();

        int iterations = 0;
        while(true) {
            iterations++;

            Map.Entry<String, List<Instance>> mostAssignmentsWorker = findWorkerWithMostAssignments(workerToAssignmentMap);

            Map.Entry<String, List<Instance>> leastAssignmentsWorker = findWorkerWithLeastAssignments(workerToAssignmentMap);

            if (mostAssignmentsWorker.getValue().size() == leastAssignmentsWorker.getValue().size()
                    || mostAssignmentsWorker.getValue().size() == leastAssignmentsWorker.getValue().size() + 1) {
                break;
            }

            String mostAssignmentsWorkerId = mostAssignmentsWorker.getKey();
            String leastAssignmentsWorkerId = leastAssignmentsWorker.getKey();

            Queue<Instance> src = (Queue) workerToAssignmentMap.get(mostAssignmentsWorkerId);
            Queue<Instance> dest = (Queue) workerToAssignmentMap.get(leastAssignmentsWorkerId);

            Instance instance = src.poll();
            Assignment newAssignment = Assignment.newBuilder()
                    .setInstance(instance)
                    .setWorkerId(leastAssignmentsWorkerId)
                    .build();
            newAssignments.add(newAssignment);

            dest.add(instance);
        }

        log.info("Rebalance - iterations: {}", iterations);

        return newAssignments;
    }

    private Map.Entry<String, List<Instance>> findWorkerWithLeastAssignments(Map<String, List<Instance>> workerToAssignmentMap) {
        return workerToAssignmentMap.entrySet().stream().min(Comparator.comparingInt(o -> o.getValue().size())).get();

    }

    private Map.Entry<String, List<Instance>> findWorkerWithMostAssignments(Map<String, List<Instance>> workerToAssignmentMap) {
        return workerToAssignmentMap.entrySet().stream().max(Comparator.comparingInt(o -> o.getValue().size())).get();
    }

>>>>>>> f773c602c... Test pr 10 (#27)
}
