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
package org.apache.pulsar.broker.admin.v3;

import io.swagger.annotations.Api;
<<<<<<< HEAD
import org.apache.pulsar.broker.admin.impl.SinkBase;
=======
import org.apache.pulsar.broker.admin.impl.SinksBase;
>>>>>>> f773c602c... Test pr 10 (#27)

import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/sink")
<<<<<<< HEAD
@Api(value = "/sink", description = "Sink admin apis", tags = "sink", hidden = true)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Sink extends SinkBase {
=======
@Api(value = "/sink", description = "Sink admin apis", tags = "sink")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Deprecated
/**
 * @deprecated in favor of {@link Sinks}
 */
public class Sink extends SinksBase {
>>>>>>> f773c602c... Test pr 10 (#27)
}
