---
id: security-tls-transport
title: Transport Encryption using TLS
sidebar_label: Transport Encryption using TLS
---

<<<<<<< HEAD
## TLS Overview

By default, Apache Pulsar clients communicate with the Apache Pulsar service in plain text, which means that all data is sent in the clear. TLS can be used to encrypt this traffic so that it cannot be snooped by a man-in-the-middle attacker.

TLS can be configured for both encryption and authentication. You may configure just TLS transport encryption, which is covered in this guide. TLS authentication is covered [elsewhere](security-tls-authentication.md). Alternatively, you can use [another authentication mechanism](security-athenz.md) on top of TLS transport encryption.

> Note that enabling TLS may have a performance impact due to encryption overhead.

## TLS concepts

TLS is a form of [public key cryptography](https://en.wikipedia.org/wiki/Public-key_cryptography). Encryption is performed using key pairs consisting of a public key and a private key. Messages are encrypted with the public key and can be decrypted with the private key.

To use TLS transport encryption, you need two kinds of key pairs, **server key pairs** and a **certificate authority**.

A third kind of key pair, **client key pairs**, are used for [client authentication](security-tls-authentication.md).

The **certificate authority** private key should be stored in a very secure location (a fully encrypted, disconnected, air gapped computer). The certificate authority public key, the **trust cert**, can be freely shared.

For both client and server key pairs, the administrator first generates a private key and a certificate request. Then the certificate authority private key is used to sign the certificate request, generating a certificate. This certificate is the public key for the server/client key pair.

For TLS transport encryption, the clients can use the **trust cert** to verify that the server they are talking to has a key pair that was signed by the certificate authority. A man-in-the-middle attacker would not have access to the certificate authority, so they couldn't create a server with such a key pair.

For TLS authentication, the server uses the **trust cert** to verify that the client has a key pair that was signed by the certificate authority. The Common Name of the **client cert** is then used as the client's role token (see [Overview](security-overview.md)).

## Creating TLS Certificates

Creating TLS certificates for Pulsar involves creating a [certificate authority](#certificate-authority) (CA), [server certificate](#server-certificate), and [client certificate](#client-certificate).

The following guide is an abridged guide to setting up a certificate authority. For a more detailed guide, there are plenty of resource on the internet. We recommend the [this guide](https://jamielinux.com/docs/openssl-certificate-authority/index.html).

### Certificate authority

The first step is to create the certificate for the CA. The CA will be used to sign both the broker and client certificates, in order to ensure that each party will trust the others. The CA should be stored in a very secure location (ideally completely disconnected from networks, air gapped, and fully encrypted).

Create a directory for your CA, and place [this openssl configuration file](https://github.com/apache/pulsar/tree/master/site2/website/static/examples/openssl.cnf) in the directory. You may want to modify the default answers for company name and department in the configuration file. Export the location of the CA directory to the environment variable, CA_HOME. The configuration file uses this environment variable to find the rest of the files and directories needed for the CA.

```bash
$ mkdir my-ca
$ cd my-ca
$ wget https://raw.githubusercontent.com/apache/pulsar/master/site2/website/static/examples/openssl.cnf
$ export CA_HOME=$(pwd)
```

Create the necessary directories, keys and certs.

```bash
$ mkdir certs crl newcerts private
$ chmod 700 private/
$ touch index.txt
$ echo 1000 > serial
$ openssl genrsa -aes256 -out private/ca.key.pem 4096
$ chmod 400 private/ca.key.pem
$ openssl req -config openssl.cnf -key private/ca.key.pem \
      -new -x509 -days 7300 -sha256 -extensions v3_ca \
      -out certs/ca.cert.pem
$ chmod 444 certs/ca.cert.pem
```

After answering the question prompts, this will store CA-related files in the `./my-ca` directory. Within that directory:

* `certs/ca.cert.pem` is the public certificate. It is meant to be distributed to all parties involved.
* `private/ca.key.pem` is the private key. This is only needed when signing a new certificate for either broker or clients and it must be safely guarded.

### Server certificate

Once a CA certificate has been created, you can create certificate requests and sign them with the CA.

The following commands will ask you a few questions and then create the certificates. When asked for the common name, you should match the hostname of the broker. You could also use a wildcard to match a group of broker hostnames, for example `*.broker.usw.example.com`. This ensures that the same certificate can be reused on multiple machines.

> #### Tips
> 
> Sometimes it is not possible or makes no sense to match the hostname,
> such as when the brokers are created with random hostnames, or you
> plan to connect to the hosts via their IP. In this case, the client
> should be configured to disable TLS hostname verification. For more
> details, see [the host verification section in client configuration](#hostname-verification).

First generate the key.
```bash
$ openssl genrsa -out broker.key.pem 2048
```

The broker expects the key to be in [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format, so convert it.

```bash
$ openssl pkcs8 -topk8 -inform PEM -outform PEM \
      -in broker.key.pem -out broker.key-pk8.pem -nocrypt
```

Generate the certificate request...

```bash
$ openssl req -config openssl.cnf \
      -key broker.key.pem -new -sha256 -out broker.csr.pem
```

... and sign it with the certificate authority.
```bash
$ openssl ca -config openssl.cnf -extensions server_cert \
      -days 1000 -notext -md sha256 \
      -in broker.csr.pem -out broker.cert.pem
```

At this point, you have a cert, `broker.cert.pem`, and a key, `broker.key-pk8.pem`, which can be used along with `ca.cert.pem` to configure TLS transport encryption for your broker and proxy nodes.

## Broker Configuration

To configure a Pulsar [broker](reference-terminology.md#broker) to use TLS transport encryption, you'll need to make some changes to `broker.conf`, which is located in the `conf` directory of your [Pulsar installation](getting-started-standalone.md).
=======
## TLS overview

By default, Apache Pulsar clients communicate with the Apache Pulsar service in plain text. This means that all data is sent in the clear. You can use TLS to encrypt this traffic to protect the traffic from the snooping of a man-in-the-middle attacker.

You can also configure TLS for both encryption and authentication. Use this guide to configure just TLS transport encryption and refer to [here](security-tls-authentication.md) for TLS authentication configuration. Alternatively, you can use [another authentication mechanism](security-athenz.md) on top of TLS transport encryption.

> Note that enabling TLS may impact the performance due to encryption overhead.

## TLS concepts

TLS is a form of [public key cryptography](https://en.wikipedia.org/wiki/Public-key_cryptography). Using key pairs consisting of a public key and a private key can perform the encryption. The public key encrpyts the messages and the private key decrypts the messages.

To use TLS transport encryption, you need two kinds of key pairs, **server key pairs** and a **certificate authority**.

You can use a third kind of key pair, **client key pairs**, for [client authentication](security-tls-authentication.md).

You should store the **certificate authority** private key in a very secure location (a fully encrypted, disconnected, air gapped computer). As for the certificate authority public key, the **trust cert**, you can freely shared it.

For both client and server key pairs, the administrator first generates a private key and a certificate request, then uses the certificate authority private key to sign the certificate request, finally generates a certificate. This certificate is the public key for the server/client key pair.

For TLS transport encryption, the clients can use the **trust cert** to verify that the server has a key pair that the certificate authority signed when the clients are talking to the server. A man-in-the-middle attacker does not have access to the certificate authority, so they couldn't create a server with such a key pair.

For TLS authentication, the server uses the **trust cert** to verify that the client has a key pair that the certificate authority signed. The common name of the **client cert** is then used as the client's role token (see [Overview](security-overview.md)).

`Bouncy Castle Provider` provides cipher suites and algorithms in Pulsar. If you need [FIPS](https://www.bouncycastle.org/fips_faq.html) version of `Bouncy Castle Provider`, please reference [Bouncy Castle page](security-bouncy-castle.md).

## Create TLS certificates

Creating TLS certificates for Pulsar involves creating a [certificate authority](#certificate-authority) (CA), [server certificate](#server-certificate), and [client certificate](#client-certificate).

Follow the guide below to set up a certificate authority. You can also refer to plenty of resources on the internet for more details. We recommend [this guide](https://jamielinux.com/docs/openssl-certificate-authority/index.html) for your detailed reference.

### Certificate authority

1. Create the certificate for the CA. You can use CA to sign both the broker and client certificates. This ensures that each party will trust the others. You should store CA in a very secure location (ideally completely disconnected from networks, air gapped, and fully encrypted).

2. Entering the following command to create a directory for your CA, and place [this openssl configuration file](https://github.com/apache/pulsar/tree/master/site2/website/static/examples/openssl.cnf) in the directory. You may want to modify the default answers for company name and department in the configuration file. Export the location of the CA directory to the environment variable, CA_HOME. The configuration file uses this environment variable to find the rest of the files and directories that the CA needs.

```bash
mkdir my-ca
cd my-ca
wget https://raw.githubusercontent.com/apache/pulsar/master/site2/website/static/examples/openssl.cnf
export CA_HOME=$(pwd)
```

3. Enter the commands below to create the necessary directories, keys and certs.

```bash
mkdir certs crl newcerts private
chmod 700 private/
touch index.txt
echo 1000 > serial
openssl genrsa -aes256 -out private/ca.key.pem 4096
chmod 400 private/ca.key.pem
openssl req -config openssl.cnf -key private/ca.key.pem \
    -new -x509 -days 7300 -sha256 -extensions v3_ca \
    -out certs/ca.cert.pem
chmod 444 certs/ca.cert.pem
```

4. After you answer the question prompts, CA-related files are stored in the `./my-ca` directory. Within that directory:

* `certs/ca.cert.pem` is the public certificate. This public certificates is meant to be distributed to all parties involved.
* `private/ca.key.pem` is the private key. You only need it when you are signing a new certificate for either broker or clients and you must safely guard this private key.

### Server certificate

Once you have created a CA certificate, you can create certificate requests and sign them with the CA.

The following commands ask you a few questions and then create the certificates. When you are asked for the common name, you should match the hostname of the broker. You can also use a wildcard to match a group of broker hostnames, for example, `*.broker.usw.example.com`. This ensures that multiple machines can reuse the same certificate.

> #### Tips
> 
> Sometimes matching the hostname is not possible or makes no sense,
> such as when you creat the brokers with random hostnames, or you
> plan to connect to the hosts via their IP. In these cases, you 
> should configure the client to disable TLS hostname verification. For more
> details, you can see [the host verification section in client configuration](#hostname-verification).

1. Enter the command below to generate the key.

```bash
openssl genrsa -out broker.key.pem 2048
```

The broker expects the key to be in [PKCS 8](https://en.wikipedia.org/wiki/PKCS_8) format, so enter the following command to convert it.

```bash
openssl pkcs8 -topk8 -inform PEM -outform PEM \
      -in broker.key.pem -out broker.key-pk8.pem -nocrypt
```

2. Enter the following command to generate the certificate request.

```bash
openssl req -config openssl.cnf \
    -key broker.key.pem -new -sha256 -out broker.csr.pem
```

3. Sign it with the certificate authority by entering the command below.

```bash
openssl ca -config openssl.cnf -extensions server_cert \
    -days 1000 -notext -md sha256 \
    -in broker.csr.pem -out broker.cert.pem
```

At this point, you have a cert, `broker.cert.pem`, and a key, `broker.key-pk8.pem`, which you can use along with `ca.cert.pem` to configure TLS transport encryption for your broker and proxy nodes.

## Broker Configuration

To configure a Pulsar [broker](reference-terminology.md#broker) to use TLS transport encryption, you need to make some changes to `broker.conf`, which locates in the `conf` directory of your [Pulsar installation](getting-started-standalone.md).
>>>>>>> f773c602c... Test pr 10 (#27)

Add these values to the configuration file (substituting the appropriate certificate paths where necessary):

```properties
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker.cert.pem
tlsKeyFilePath=/path/to/broker.key-pk8.pem
tlsTrustCertsFilePath=/path/to/ca.cert.pem
```

<<<<<<< HEAD
> A full list of parameters available in the `conf/broker.conf` file,
> as well as the default values for those parameters, can be found in [Broker Configuration](reference-configuration.md#broker) 

### TLS Protocol Version and Cipher

The broker (and proxy) can be configured to require specific TLS protocol versions and ciphers for TLS negiotation. This can be used to stop clients from requesting downgraded TLS protocol versions or ciphers which may have weaknesses.

Both the TLS protocol versions and cipher properties can take multiple values, separated by commas. The possible values for protocol version and ciphers depend on the TLS provider being used. Pulsar uses OpenSSL if available, but if not defaults back to the JDK implementation.
=======
> You can find a full list of parameters available in the `conf/broker.conf` file,
> as well as the default values for those parameters, in [Broker Configuration](reference-configuration.md#broker) 
> 
### TLS Protocol Version and Cipher

You can configure the broker (and proxy) to require specific TLS protocol versions and ciphers for TLS negiotation. You can use the TLS protocol versions and ciphers to stop clients from requesting downgraded TLS protocol versions or ciphers that may have weaknesses.

Both the TLS protocol versions and cipher properties can take multiple values, separated by commas. The possible values for protocol version and ciphers depend on the TLS provider that you are using. Pulsar uses OpenSSL if the OpenSSL is available, but if the OpenSSL is not available, Pulsar defaults back to the JDK implementation.
>>>>>>> f773c602c... Test pr 10 (#27)

```properties
tlsProtocols=TLSv1.2,TLSv1.1
tlsCiphers=TLS_DH_RSA_WITH_AES_256_GCM_SHA384,TLS_DH_RSA_WITH_AES_256_CBC_SHA
```

<<<<<<< HEAD
OpenSSL currently supports ```SSL2```, ```SSL3```, ```TLSv1```, ```TLSv1.1``` and ```TLSv1.2``` for the protocol version. A list of supported cipher can be acquired from the openssl ciphers command, i.e. ```openssl ciphers -tls_v2```.

For JDK 8, a list of supported values can be obtained from the documentation:
=======
OpenSSL currently supports ```SSL2```, ```SSL3```, ```TLSv1```, ```TLSv1.1``` and ```TLSv1.2``` for the protocol version. You can acquire a list of supported cipher from the openssl ciphers command, i.e. ```openssl ciphers -tls_v2```.

For JDK 8, you can obtain a list of supported values from the documentation:
>>>>>>> f773c602c... Test pr 10 (#27)
- [TLS protocol](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#SSLContext)
- [Ciphers](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites)

## Proxy Configuration

<<<<<<< HEAD
Proxies need to configure TLS in two directions, for clients connecting to the proxy, and for the proxy to be able to connect to brokers.
=======
Proxies need to configure TLS in two directions, for clients connecting to the proxy, and for the proxy connecting to brokers.
>>>>>>> f773c602c... Test pr 10 (#27)

```properties
# For clients connecting to the proxy
tlsEnabledInProxy=true
tlsCertificateFilePath=/path/to/broker.cert.pem
tlsKeyFilePath=/path/to/broker.key-pk8.pem
tlsTrustCertsFilePath=/path/to/ca.cert.pem

# For the proxy to connect to brokers
tlsEnabledWithBroker=true
brokerClientTrustCertsFilePath=/path/to/ca.cert.pem
```

## Client configuration

<<<<<<< HEAD
When TLS transport encryption is enabled, you need to configure the client to use ```https://``` and port 8443 for the web service URL, and ```pulsar+ssl://``` and port 6651 for the broker service URL.

As the server certificate you generated above doesn't belong to any of the default trust chains, you also need to either specify the path the **trust cert** (recommended), or tell the client to allow untrusted server certs.

#### Hostname verification

Hostname verification is a TLS security feature whereby a client can refuse to connect to a server if the "CommonName" does not match the hostname to which it is connecting. By default, Pulsar clients disable hostname verification, as it requires that each broker has a DNS record and a unique cert.

Moreover, as the administrator has full control of the certificate authority, it is unlikely that a bad actor would be able to pull off a man-in-the-middle attack. "allowInsecureConnection" allows the client to connect to servers whose cert has not been signed by an approved CA. The client disables it by default, and should always be disabled in production environments. As long as "allowInsecureConnection" is disabled, a man-in-the-middle attack would require that the attacker has access to the CA.

One scenario where you may want to enable hostname verification is where you have multiple proxy nodes behind a VIP, and the VIP has a DNS record, for example, pulsar.mycompany.com. In this case, you can generate a TLS cert with pulsar.mycompany.com as the "CommonName," and then enable hostname verification on the client.

The examples below show hostname verification being disabled for the Java client, though you can be omit this as the client disables it by default. C++/python clients do now allow this to be configured at the moment.

### CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools#pulsar-admin), [`pulsar-perf`](reference-cli-tools#pulsar-perf), and [`pulsar-client`](reference-cli-tools#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You'll need to add the following parameters to that file to use TLS transport with Pulsar's CLI tools:
=======
When you enable the TLS transport encryption, you need to configure the client to use ```https://``` and port 8443 for the web service URL, and ```pulsar+ssl://``` and port 6651 for the broker service URL.

As the server certificate that you generated above does not belong to any of the default trust chains, you also need to either specify the path the **trust cert** (recommended), or tell the client to allow untrusted server certs.

### Hostname verification

Hostname verification is a TLS security feature whereby a client can refuse to connect to a server if the "CommonName" does not match the hostname to which the hostname is connecting. By default, Pulsar clients disable hostname verification, as it requires that each broker has a DNS record and a unique cert.

Moreover, as the administrator has full control of the certificate authority, a bad actor is unlikely to be able to pull off a man-in-the-middle attack. "allowInsecureConnection" allows the client to connect to servers whose cert has not been signed by an approved CA. The client disables "allowInsecureConnection" by default, and you should always disable "allowInsecureConnection" in production environments. As long as you disable "allowInsecureConnection", a man-in-the-middle attack requires that the attacker has access to the CA.

One scenario where you may want to enable hostname verification is where you have multiple proxy nodes behind a VIP, and the VIP has a DNS record, for example, pulsar.mycompany.com. In this case, you can generate a TLS cert with pulsar.mycompany.com as the "CommonName," and then enable hostname verification on the client.

The examples below show hostname verification being disabled for the Java client, though you can omit this as the client disables the hostname verification by default. C++/python/Node.js clients do now allow configuring this at the moment.

### CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](reference-cli-tools.md#pulsar-admin), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` config file in a Pulsar installation.

You need to add the following parameters to that file to use TLS transport with the CLI tools of Pulsar:
>>>>>>> f773c602c... Test pr 10 (#27)

```properties
webServiceUrl=https://broker.example.com:8443/
brokerServiceUrl=pulsar+ssl://broker.example.com:6651/
useTls=true
tlsAllowInsecureConnection=false
tlsTrustCertsFilePath=/path/to/ca.cert.pem
tlsEnableHostnameVerification=false
```

<<<<<<< HEAD
### Java client
=======
#### Java client
>>>>>>> f773c602c... Test pr 10 (#27)

```java
import org.apache.pulsar.client.api.PulsarClient;

PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar+ssl://broker.example.com:6651/")
    .enableTls(true)
    .tlsTrustCertsFilePath("/path/to/ca.cert.pem")
    .enableTlsHostnameVerification(false) // false by default, in any case
    .allowTlsInsecureConnection(false) // false by default, in any case
    .build();
```

<<<<<<< HEAD
### Python client
=======
#### Python client
>>>>>>> f773c602c... Test pr 10 (#27)

```python
from pulsar import Client

client = Client("pulsar+ssl://broker.example.com:6651/",
<<<<<<< HEAD
=======
                tls_hostname_verification=True,
>>>>>>> f773c602c... Test pr 10 (#27)
                tls_trust_certs_file_path="/path/to/ca.cert.pem",
                tls_allow_insecure_connection=False) // defaults to false from v2.2.0 onwards
```

<<<<<<< HEAD
### C++ client
=======
#### C++ client
>>>>>>> f773c602c... Test pr 10 (#27)

```c++
#include <pulsar/Client.h>

<<<<<<< HEAD
pulsar::ClientConfiguration config;
config.setUseTls(true);
config.setTlsTrustCertsFilePath("/path/to/ca.cert.pem");
config.setTlsAllowInsecureConnection(false); // defaults to false from v2.2.0 onwards

pulsar::Client client("pulsar+ssl://broker.example.com:6651/", config);
=======
ClientConfiguration config = ClientConfiguration();
config.setUseTls(true);  // shouldn't be needed soon
config.setTlsTrustCertsFilePath(caPath);
config.setTlsAllowInsecureConnection(false);
config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));
config.setValidateHostName(true);
```

#### Node.js client

```JavaScript
const Pulsar = require('pulsar-client');

(async () => {
  const client = new Pulsar.Client({
    serviceUrl: 'pulsar+ssl://broker.example.com:6651/',
    tlsTrustCertsFilePath: '/path/to/ca.cert.pem',
  });
})();
```

#### C# client

```c#
var certificate = new X509Certificate2("ca.cert.pem");
var client = PulsarClient.Builder()
                         .TrustedCertificateAuthority(certificate) //If the CA is not trusted on the host, you can add it explicitly.
                         .VerifyCertificateAuthority(true) //Default is 'true'
                         .VerifyCertificateName(false)     //Default is 'false'
                         .Build();
>>>>>>> f773c602c... Test pr 10 (#27)
```
