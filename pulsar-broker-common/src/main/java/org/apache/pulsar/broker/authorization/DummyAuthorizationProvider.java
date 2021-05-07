package org.apache.pulsar.broker.authorization;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;

public class DummyAuthorizationProvider implements AuthorizationProvider {
    @Override
    public void initialize(ServiceConfiguration conf, ConfigurationCacheService configCache) throws IOException {

    }

    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role, AuthenticationDataSource authenticationData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role, AuthenticationDataSource authenticationData, String subscription) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role, AuthenticationDataSource authenticationData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions, String role, String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName, Set<String> roles, String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName, String role, String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions, String role, String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() throws IOException {

    }
}
