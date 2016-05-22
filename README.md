# cloudex

[![Build Status](https://travis-ci.org/omerio/cloudex.svg)](https://travis-ci.org/omerio/cloudex)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/15151a0352eb4e16870e792d5a143add)](https://www.codacy.com/app/omer-dawelbeit/cloudex)
[![Coverage Status](https://coveralls.io/repos/github/omerio/cloudex/badge.svg?branch=master)](https://coveralls.io/github/omerio/cloudex?branch=master)
[![Dependency Status](https://www.versioneye.com/user/projects/5724d152ba37ce0031fc218e/badge.svg?style=flat)](https://www.versioneye.com/user/projects/5724d152ba37ce0031fc218e)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.cloudex/cloudex-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.cloudex/cloudex-core)
[![Join the chat at https://gitter.im/omerio/cloudex](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/omerio/cloudex)

[CloudEx](http://cloudex.io) is a framework for executing jobs on cloud virtual machines. CloudEx uses a coordinator component and processor VMs to facilitate the excecution of jobs in the cloud.

## Getting Started

To build the libraries:

```bash
    git clone https://github.com/omerio/cloudex.git
    cd cloudex
    mvn install
```   

### Requirements

Java 7 or above.

### Usage

Include the following Maven dependency for the core library:

```xml
<dependency>
  <groupId>io.cloudex</groupId>
  <artifactId>cloudex-core</artifactId>
  <version>1.0.0</version>
</dependency>
```

Then include the dependency for the cloud provider specific library (e.g. Google Cloud Platform)

```xml
<dependency>
  <groupId>io.cloudex</groupId>
  <artifactId>cloudex-google</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Documentations

To find out more about CloudEx, check out the [documentation](https://github.com/omerio/cloudex/wiki).

## Contributing

See the [CONTRIBUTING Guidelines](https://github.com/omerio/cloudex/blob/master/CONTRIBUTING.md)

## Support
If you have any problem or suggestion please open an issue [here](https://github.com/omerio/cloudex/issues).

## License
Apache 2.0 - See [LICENSE](https://github.com/omerio/cloudex/blob/master/LICENSE) for more information.
