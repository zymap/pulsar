---
id: administration-dashboard
<<<<<<< HEAD
title: The Pulsar dashboard
sidebar_label: Dashboard
---

The Pulsar dashboard is a web application that enables users to monitor current stats for all [topics](reference-terminology.md#topic) in tabular form.

The dashboard is a data collector that polls stats from all the brokers in a Pulsar instance (across multiple clusters) and stores all the information in a [PostgreSQL](https://www.postgresql.org/) database.

A [Django](https://www.djangoproject.com) web app is used to render the collected data.

## Install

The easiest way to use the dashboard is to run it inside a [Docker](https://www.docker.com/products/docker) container. A {@inject: github:`Dockerfile`:/dashboard/Dockerfile} to generate the image is provided.

To generate the Docker image:
=======
title: Pulsar dashboard
sidebar_label: Dashboard
---

> Note   
> Pulsar dashboard is deprecated. If you want to manage and monitor the stats of your topics, use [Pulsar Manager](administration-pulsar-manager.md). 

Pulsar dashboard is a web application that enables users to monitor current stats for all [topics](reference-terminology.md#topic) in tabular form.

The dashboard is a data collector that polls stats from all the brokers in a Pulsar instance (across multiple clusters) and stores all the information in a [PostgreSQL](https://www.postgresql.org/) database.

You can use the [Django](https://www.djangoproject.com) web app to render the collected data.

## Install

The easiest way to use the dashboard is to run it inside a [Docker](https://www.docker.com/products/docker) container.

```shell
$ SERVICE_URL=http://broker.example.com:8080/
$ docker run -p 80:80 \
  -e SERVICE_URL=$SERVICE_URL \
  apachepulsar/pulsar-dashboard:{{pulsar:version}}
```

You can find the {@inject: github:`Dockerfile`:/dashboard/Dockerfile} in the `dashboard` directory and build an image from scratch as well:
>>>>>>> f773c602c... Test pr 10 (#27)

```shell
$ docker build -t apachepulsar/pulsar-dashboard dashboard
```

<<<<<<< HEAD
To run the dashboard:

```shell
$ SERVICE_URL=http://broker.example.com:8080/
$ docker run -p 80:80 \
  -e SERVICE_URL=$SERVICE_URL \
  apachepulsar/pulsar-dashboard
```

You need to specify only one service URL for a Pulsar cluster. Internally, the collector will figure out all the existing clusters and the brokers from where it needs to pull the metrics. If you're connecting the dashboard to Pulsar running in standalone mode, the URL will be `http://<broker-ip>:8080` by default. `<broker-ip>` is the ip address or hostname of the machine running Pulsar standalone. The ip address or hostname should be accessible from the docker instance running dashboard.

Once the Docker container is running, the web dashboard will be accessible via `localhost` or whichever host is being used by Docker.

> The `SERVICE_URL` that the dashboard uses needs to be reachable from inside the Docker container

If the Pulsar service is running in standalone mode in `localhost`, the `SERVICE_URL` would have to
be the IP of the machine.

Similarly, given the Pulsar standalone advertises itself with localhost by default, we need to
=======
If token authentication is enabled:
> Provided token should have super-user access. 
```shell
$ SERVICE_URL=http://broker.example.com:8080/
$ JWT_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c
$ docker run -p 80:80 \
  -e SERVICE_URL=$SERVICE_URL \
  -e JWT_TOKEN=$JWT_TOKEN \
  apachepulsar/pulsar-dashboard
```
 
You need to specify only one service URL for a Pulsar cluster. Internally, the collector figures out all the existing clusters and the brokers from where it needs to pull the metrics. If you connect the dashboard to Pulsar running in standalone mode, the URL is `http://<broker-ip>:8080` by default. `<broker-ip>` is the ip address or hostname of the machine running Pulsar standalone. The ip address or hostname should be accessible from the docker instance running dashboard.

Once the Docker container runs, the web dashboard is accessible via `localhost` or whichever host that Docker uses.

> The `SERVICE_URL` that the dashboard uses needs to be reachable from inside the Docker container

If the Pulsar service runs in standalone mode in `localhost`, the `SERVICE_URL` has to
be the IP of the machine.

Similarly, given the Pulsar standalone advertises itself with localhost by default, you need to
>>>>>>> f773c602c... Test pr 10 (#27)
explicitely set the advertise address to the host IP. For example:

```shell
$ bin/pulsar standalone --advertised-address 1.2.3.4
```

### Known issues

<<<<<<< HEAD
Pulsar [authentication](security-overview.md#authentication-providers) is not supported at this point. The dashboard's data collector does not pass any authentication-related data and will be denied access if the Pulsar broker requires authentication.
=======
Currently, only Pulsar Token [authentication](security-overview.md#authentication-providers) is supported.
>>>>>>> f773c602c... Test pr 10 (#27)
