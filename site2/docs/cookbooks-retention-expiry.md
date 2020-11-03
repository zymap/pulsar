---
id: cookbooks-retention-expiry
title: Message retention and expiry
sidebar_label: Message retention and expiry
---

<<<<<<< HEAD
Pulsar brokers are responsible for handling messages that pass through Pulsar, including [persistent storage](concepts-architecture-overview.md#persistent-storage) of messages. By default, brokers:

* immediately delete all messages that have been acknowledged on every subscription, and
* persistently store all unacknowledged messages in a [backlog](#backlog-quotas).

In Pulsar, you can override both of these default behaviors, at the namespace level, in two ways:

* You can persistently store messages that have already been consumed and acknowledged for a minimum time by setting [retention policies](#retention-policies).
* Messages that are not acknowledged within a specified timeframe, can be automatically marked as consumed, by specifying the [time to live](#time-to-live-ttl) (TTL).

Pulsar's [admin interface](admin-api-overview.md) enables you to manage both retention policies and TTL at the namespace level (and thus within a specific tenant and either on a specific cluster or in the [`global`](concepts-architecture-overview.md#global-cluster) cluster).


> #### Retention and TTL are solving two different problems
> * Message retention: Keep the data for at least X hours (even if acknowledged)
> * Time-to-live: Discard data after some time (by automatically acknowledging)
>
> In most cases, applications will want to use either one or the other (or none). 
=======
Pulsar brokers are responsible for handling messages that pass through Pulsar, including [persistent storage](concepts-architecture-overview.md#persistent-storage) of messages. By default, for each topic, brokers only retain messages that are in at least one backlog. A backlog is the set of unacknowledged messages for a particular subscription. As a topic can have multiple subscriptions, a topic can have multiple backlogs.

As a consequence, no messages are retained (by default) on a topic that has not had any subscriptions created for it.

(Note that messages that are no longer being stored are not necessarily immediately deleted, and may in fact still be accessible until the next ledger rollover. Because clients cannot predict when rollovers may happen, it is not wise to rely on a rollover not happening at an inconvenient point in time.)

In Pulsar, you can modify this behavior, with namespace granularity, in two ways:

* You can persistently store messages that are not within a backlog (because they've been acknowledged by on every existing subscription, or because there are no subscriptions) by setting [retention policies](#retention-policies).
* Messages that are not acknowledged within a specified timeframe can be automatically acknowledged, by specifying the [time to live](#time-to-live-ttl) (TTL).

Pulsar's [admin interface](admin-api-overview.md) enables you to manage both retention policies and TTL with namespace granularity (and thus within a specific tenant and either on a specific cluster or in the [`global`](concepts-architecture-overview.md#global-cluster) cluster).


> #### Retention and TTL solve two different problems
> * Message retention: Keep the data for at least X hours (even if acknowledged)
> * Time-to-live: Discard data after some time (by automatically acknowledging)
>
> Most applications will want to use at most one of these.
>>>>>>> f773c602c... Test pr 10 (#27)


## Retention policies

<<<<<<< HEAD
By default, when a Pulsar message arrives at a broker it will be stored until it has been acknowledged by a consumer, at which point it will be deleted. You can override this behavior and retain even messages that have already been acknowledged by setting a *retention policy* on all the topics in a given namespace. When you set a retention policy you can set either a *size limit* or a *time limit*.

When you set a size limit of, say, 10 gigabytes, then messages in all topics in the namespace, *even acknowledged messages*, will be retained until the size limit for the topic is reached; if you set a time limit of, say, 1 day, then messages for all topics in the namespace will be retained for 24 hours.

It is also possible to set *infinite* retention time or size, by setting `-1` for either time or
size retention.
=======
By default, when a Pulsar message arrives at a broker it will be stored until it has been acknowledged on all subscriptions, at which point it will be marked for deletion. You can override this behavior and retain even messages that have already been acknowledged on all subscriptions by setting a *retention policy* for all topics in a given namespace. Retention is based on both a *size limit* and a *time limit*.

Retention policies are particularly useful if you intend to exclusively use the Reader interface. Because the Reader interface does not use acknowledgements, messages will never exist within backlogs. Most realistic Reader-only use cases require that retention be configured.

When you set a retention policy you must set **both** a *size limit* and a *time limit*. In the case where you don't want to limit by either time or set, the value must be set to `-1`. Retention policy will be effectively disabled and it won't prevent the deletion of acknowledged messages when either size or time limit is set to `0`. Infinite retention can be achieved by setting both time and size limits to `-1`.

When you set a size limit of, say, 10 gigabytes, and the time limit to `-1` then acknowledged messages in all topics in the namespace will be retained until the size limit for the topic is reached; if you set a time limit of, say, 1 day, and the size limit to `-1` then acknowledged messages for all topics in the namespace will be retained for 24 hours.

The retention settings apply to all messages on topics that do not have any subscriptions, or if there are subscriptions, to messages that have been acked by all subscriptions. The retention policy settings do not affect unacknowledged messages on topics with subscriptions -- these are instead controlled by the backlog quota (see below).

When a retention limit is exceeded, the oldest message is marked for deletion until the set of retained messages falls within the specified limits again.
>>>>>>> f773c602c... Test pr 10 (#27)

### Defaults

There are two configuration parameters that you can use to set [instance](reference-terminology.md#instance)-wide defaults for message retention: [`defaultRetentionTimeInMinutes=0`](reference-configuration.md#broker-defaultRetentionTimeInMinutes) and [`defaultRetentionSizeInMB=0`](reference-configuration.md#broker-defaultRetentionSizeInMB).

Both of these parameters are in the [`broker.conf`](reference-configuration.md#broker) configuration file.

### Set retention policy

You can set a retention policy for a namespace by specifying the namespace as well as both a size limit *and* a time limit.

#### pulsar-admin

<<<<<<< HEAD
Use the [`set-retention`](reference-pulsar-admin.md#namespaces-set-retention) subcommand and specify a namespace, a size limit using the `-s`/`--size` flag, and a time limit using the `-t`/`--time` flag.
=======
Use the [`set-retention`](reference-pulsar-admin.md#namespaces-set-retention) subcommand and specify a namespace, a size limit using the `-s`/`--size` flag, and a time limit using the `-t`/`--time` flag. 

You must set **both** a *size limit* and a *time limit*. In the case where you don't want to limit by either time or set, the value must be set to `-1`. Retention policy will be effectively disabled and it won't prevent the deletion of acknowledged messages when either size or time limit is set to 0.
>>>>>>> f773c602c... Test pr 10 (#27)

##### Examples

To set a size limit of 10 gigabytes and a time limit of 3 hours for the `my-tenant/my-ns` namespace:

```shell
$ pulsar-admin namespaces set-retention my-tenant/my-ns \
  --size 10G \
  --time 3h
```

<<<<<<< HEAD
To set retention with infinite time and a size limit:
=======
To set retention where time limit is ignored and the size limit of 1 terabyte determines retention:
>>>>>>> f773c602c... Test pr 10 (#27)

```shell
$ pulsar-admin namespaces set-retention my-tenant/my-ns \
  --size 1T \
  --time -1
```

<<<<<<< HEAD
Similarly, even the size can be to unlimited:
=======
To set retention where size limit is ignored and the time limit of 3 hours determines retention:

```shell
$ pulsar-admin namespaces set-retention my-tenant/my-ns \
  --size -1 \
  --time 3h
```

To set infinite retention:
>>>>>>> f773c602c... Test pr 10 (#27)

```shell
$ pulsar-admin namespaces set-retention my-tenant/my-ns \
  --size -1 \
  --time -1
```

<<<<<<< HEAD
=======
To disable the retention policy

```shell
$ pulsar-admin namespaces set-retention my-tenant/my-ns \
  --size 0 \
  --time 0
```
>>>>>>> f773c602c... Test pr 10 (#27)


#### REST API

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/retention|operation/setRetention}

#### Java

```java
int retentionTime = 10; // 10 minutes
int retentionSize = 500; // 500 megabytes
RetentionPolicies policies = new RetentionPolicies(retentionTime, retentionSize);
admin.namespaces().setRetention(namespace, policies);
```

### Get retention policy

You can fetch the retention policy for a namespace by specifying the namespace. The output will be a JSON object with two keys: `retentionTimeInMinutes` and `retentionSizeInMB`.

#### pulsar-admin

Use the [`get-retention`](reference-pulsar-admin.md#namespaces) subcommand and specify the namespace.

##### Example

```shell
$ pulsar-admin namespaces get-retention my-tenant/my-ns
{
  "retentionTimeInMinutes": 10,
<<<<<<< HEAD
  "retentionSizeInMB": 0
=======
  "retentionSizeInMB": 500
>>>>>>> f773c602c... Test pr 10 (#27)
}
```

#### REST API

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/retention|operation/getRetention}

#### Java

```java
admin.namespaces().getRetention(namespace);
```

## Backlog quotas

*Backlogs* are sets of unacknowledged messages for a topic that have been stored by bookies. Pulsar stores all unacknowledged messages in backlogs until they are processed and acknowledged.

You can control the allowable size of backlogs, at the namespace level, using *backlog quotas*. Setting a backlog quota involves setting:

<<<<<<< HEAD
=======
TODO: Expand on is this per backlog or per topic?

>>>>>>> f773c602c... Test pr 10 (#27)
* an allowable *size threshold* for each topic in the namespace
* a *retention policy* that determines which action the [broker](reference-terminology.md#broker) takes if the threshold is exceeded.

The following retention policies are available:

Policy | Action
:------|:------
`producer_request_hold` | The broker will hold and not persist produce request payload
`producer_exception` | The broker will disconnect from the client by throwing an exception
`consumer_backlog_eviction` | The broker will begin discarding backlog messages


> #### Beware the distinction between retention policy types
<<<<<<< HEAD
> As you may have noticed, there are two definitions of the term "retention policy" in Pulsar, one that applies to persistent storage of already-acknowledged messages and one that applies to backlogs.
=======
> As you may have noticed, there are two definitions of the term "retention policy" in Pulsar, one that applies to persistent storage of messages not in backlogs, and one that applies to messages within backlogs.
>>>>>>> f773c602c... Test pr 10 (#27)


Backlog quotas are handled at the namespace level. They can be managed via:

### Set size thresholds and backlog retention policies

You can set a size threshold and backlog retention policy for all of the topics in a [namespace](reference-terminology.md#namespace) by specifying the namespace, a size limit, and a policy by name.

#### pulsar-admin

Use the [`set-backlog-quota`](reference-pulsar-admin.md#namespaces) subcommand and specify a namespace, a size limit using the `-l`/`--limit` flag, and a retention policy using the `-p`/`--policy` flag.

##### Example

```shell
$ pulsar-admin namespaces set-backlog-quota my-tenant/my-ns \
  --limit 2G \
  --policy producer_request_hold
```

#### REST API

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/backlogQuota|operation/getBacklogQuotaMap}

#### Java

```java
long sizeLimit = 2147483648L;
BacklogQuota.RetentionPolicy policy = BacklogQuota.RetentionPolicy.producer_request_hold;
BacklogQuota quota = new BacklogQuota(sizeLimit, policy);
admin.namespaces().setBacklogQuota(namespace, quota);
```

### Get backlog threshold and backlog retention policy

You can see which size threshold and backlog retention policy has been applied to a namespace.

#### pulsar-admin

Use the [`get-backlog-quotas`](reference-pulsar-admin.md#pulsar-admin-namespaces-get-backlog-quotas) subcommand and specify a namespace. Here's an example:

```shell
$ pulsar-admin namespaces get-backlog-quotas my-tenant/my-ns
{
  "destination_storage": {
    "limit" : 2147483648,
    "policy" : "producer_request_hold"
  }
}
```

#### REST API

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/backlogQuotaMap|operation/getBacklogQuotaMap}

#### Java

```java
Map<BacklogQuota.BacklogQuotaType,BacklogQuota> quotas =
  admin.namespaces().getBacklogQuotas(namespace);
```

### Remove backlog quotas

#### pulsar-admin

<<<<<<< HEAD
Use the [`remove-backlog-quotas`](reference-pulsar-admin.md#pulsar-admin-namespaces-remove-backlog-quota) subcommand and specify a namespace. Here's an example:

```shell
$ pulsar-admin namespaces remove-backlog-quotas my-tenant/my-ns
=======
Use the [`remove-backlog-quota`](reference-pulsar-admin.md#pulsar-admin-namespaces-remove-backlog-quota) subcommand and specify a namespace. Here's an example:

```shell
$ pulsar-admin namespaces remove-backlog-quota my-tenant/my-ns
>>>>>>> f773c602c... Test pr 10 (#27)
```

#### REST API

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/backlogQuota|operation/removeBacklogQuota}

#### Java

```java
admin.namespaces().removeBacklogQuota(namespace);
```

### Clear backlog

#### pulsar-admin

Use the [`clear-backlog`](reference-pulsar-admin.md#pulsar-admin-namespaces-clear-backlog) subcommand.

##### Example

```shell
$ pulsar-admin namespaces clear-backlog my-tenant/my-ns
```

By default, you will be prompted to ensure that you really want to clear the backlog for the namespace. You can override the prompt using the `-f`/`--force` flag.

## Time to live (TTL)

By default, Pulsar stores all unacknowledged messages forever. This can lead to heavy disk space usage in cases where a lot of messages are going unacknowledged. If disk space is a concern, you can set a time to live (TTL) that determines how long unacknowledged messages will be retained.

### Set the TTL for a namespace

#### pulsar-admin

Use the [`set-message-ttl`](reference-pulsar-admin.md#pulsar-admin-namespaces-set-message-ttl) subcommand and specify a namespace and a TTL (in seconds) using the `-ttl`/`--messageTTL` flag.

##### Example

```shell
$ pulsar-admin namespaces set-message-ttl my-tenant/my-ns \
  --messageTTL 120 # TTL of 2 minutes
```

#### REST API

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/setNamespaceMessageTTL}

#### Java

```java
admin.namespaces().setNamespaceMessageTTL(namespace, ttlInSeconds);
```

### Get the TTL configuration for a namespace

#### pulsar-admin

Use the [`get-message-ttl`](reference-pulsar-admin.md#pulsar-admin-namespaces-get-message-ttl) subcommand and specify a namespace.

##### Example

```shell
$ pulsar-admin namespaces get-message-ttl my-tenant/my-ns
60
```

#### REST API

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/getNamespaceMessageTTL}

#### Java

```java
admin.namespaces().getNamespaceMessageTTL(namespace)
```

