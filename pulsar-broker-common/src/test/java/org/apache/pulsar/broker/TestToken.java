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
package org.apache.pulsar.broker;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;

public class TestToken {
    @Test
    public void testTOken() throws IOException {
        String token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJzdXBlci11c2VyIn0.DA21v370Q5Je2RlJIDZhbYmQ7S0Ziudt1gvnuZ60VvynCSVN46cyZjf2339zT076-BTlGbzbPo7BLIem_BFFXGvl5tux_MJ2TDExqaqvsXkwFkI1wbELt5G8A2r-_27loC1Bhh9ttOGoxRwQIxR6h3Sj8_J3vMysipEqwoecT4MnCGOZdLpAwKRWBzrNVzHXY86Cucvwir6at32yhCHTLISYP2zrC33Z8-89uZGLYz_yfCJOhq4CnY1oaf7u1pu05QMWCJdSSdAAwfI_bVE8BxAS4fmM9GxK-gUVo6xHfFHRbnuA6rVfm1SI1PRmVP4145m1wflTobuS8npLpQO4Ew";
//        byte[] data = Files.readAllBytes(Paths.get("/Users/zhangyong/github.com/streamnative/streamnative-tests/config/pulsar/auth/token/public.key"));
        byte[] validationKey = AuthTokenUtils.readKeyFromUrl("file:///Users/zhangyong/github.com/streamnative/streamnative-tests/config/pulsar/auth/token/public.key");
        Key secret = AuthTokenUtils.decodeSecretKey(validationKey);
        Jwt<?, Claims> jwt = Jwts.parser()
            .setSigningKey(secret)
            .parse(token);
    }
}
