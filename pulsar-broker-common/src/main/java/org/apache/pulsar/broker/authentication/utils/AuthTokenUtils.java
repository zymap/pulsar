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
package org.apache.pulsar.broker.authentication.utils;

import com.google.common.io.ByteStreams;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
<<<<<<< HEAD
=======
import io.jsonwebtoken.io.DecodingException;
>>>>>>> f773c602c... Test pr 10 (#27)
import io.jsonwebtoken.io.Encoders;
import io.jsonwebtoken.security.Keys;

import java.io.IOException;
import java.io.InputStream;
<<<<<<< HEAD
=======
import java.nio.file.Files;
import java.nio.file.Paths;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.security.Key;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Date;
import java.util.Optional;

import javax.crypto.SecretKey;

import lombok.experimental.UtilityClass;

<<<<<<< HEAD
=======
import org.apache.commons.codec.binary.Base64;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.url.URL;

@UtilityClass
public class AuthTokenUtils {

    public static SecretKey createSecretKey(SignatureAlgorithm signatureAlgorithm) {
        return Keys.secretKeyFor(signatureAlgorithm);
    }

    public static SecretKey decodeSecretKey(byte[] secretKey) {
        return Keys.hmacShaKeyFor(secretKey);
    }

<<<<<<< HEAD
    public static PrivateKey decodePrivateKey(byte[] key) throws IOException {
        try {
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(key);
            KeyFactory kf = KeyFactory.getInstance("RSA");
=======
    public static PrivateKey decodePrivateKey(byte[] key, SignatureAlgorithm algType) throws IOException {
        try {
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(key);
            KeyFactory kf = KeyFactory.getInstance(keyTypeForSignatureAlgorithm(algType));
>>>>>>> f773c602c... Test pr 10 (#27)
            return kf.generatePrivate(spec);
        } catch (Exception e) {
            throw new IOException("Failed to decode private key", e);
        }
    }

<<<<<<< HEAD
    public static PublicKey decodePublicKey(byte[] key) throws IOException {
        try {
            X509EncodedKeySpec spec = new X509EncodedKeySpec(key);
            KeyFactory kf = KeyFactory.getInstance("RSA");
=======

    public static PublicKey decodePublicKey(byte[] key, SignatureAlgorithm algType) throws IOException {
        try {
            X509EncodedKeySpec spec = new X509EncodedKeySpec(key);
            KeyFactory kf = KeyFactory.getInstance(keyTypeForSignatureAlgorithm(algType));
>>>>>>> f773c602c... Test pr 10 (#27)
            return kf.generatePublic(spec);
        } catch (Exception e) {
            throw new IOException("Failed to decode public key", e);
        }
    }

<<<<<<< HEAD
=======
    private static String keyTypeForSignatureAlgorithm(SignatureAlgorithm alg) {
        if (alg.getFamilyName().equals("RSA")) {
            return "RSA";
        } else if (alg.getFamilyName().equals("ECDSA")) {
            return "EC";
        } else {
            String msg = "The " + alg.name() + " algorithm does not support Key Pairs.";
            throw new IllegalArgumentException(msg);
        }
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    public static String encodeKeyBase64(Key key) {
        return Encoders.BASE64.encode(key.getEncoded());
    }

    public static String createToken(Key signingKey, String subject, Optional<Date> expiryTime) {
        JwtBuilder builder = Jwts.builder()
                .setSubject(subject)
                .signWith(signingKey);

<<<<<<< HEAD
        if (expiryTime.isPresent()) {
            builder.setExpiration(expiryTime.get());
        }
=======
        expiryTime.ifPresent(builder::setExpiration);
>>>>>>> f773c602c... Test pr 10 (#27)

        return builder.compact();
    }

    public static byte[] readKeyFromUrl(String keyConfUrl) throws IOException {
        if (keyConfUrl.startsWith("data:") || keyConfUrl.startsWith("file:")) {
            try {
                return ByteStreams.toByteArray((InputStream) new URL(keyConfUrl).getContent());
            } catch (Exception e) {
                throw new IOException(e);
            }
<<<<<<< HEAD
        } else {
            // Assume the key content was passed in base64
            return Decoders.BASE64.decode(keyConfUrl);
=======
        } else if (Files.exists(Paths.get(keyConfUrl))) {
            // Assume the key content was passed in a valid file path
            try {
                return Files.readAllBytes(Paths.get(keyConfUrl));
            } catch (IOException e) {
                throw new IOException(e);
            }
        } else if (Base64.isBase64(keyConfUrl.getBytes())) {
            // Assume the key content was passed in base64
            try {
                return Decoders.BASE64.decode(keyConfUrl);
            } catch (DecodingException e) {
                String msg = "Illegal base64 character or Key file " + keyConfUrl + " doesn't exist";
                throw new IOException(msg, e);
            }
        } else {
            String msg = "Secret/Public Key file " + keyConfUrl + " doesn't exist";
            throw new IllegalArgumentException(msg);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }
}
