<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Pravega [![Build Status](https://github.com/pravega/pravega/workflows/build/badge.svg?branch=master)](https://github.com/pravega/pravega/actions?query=branch%3Amaster) [![codecov](https://codecov.io/gh/pravega/pravega/branch/master/graph/badge.svg?token=6xOvaR0sIa)](https://codecov.io/gh/pravega/pravega) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Version](https://img.shields.io/github/release/pravega/pravega.svg)](https://github.com/pravega/pravega/releases) [![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4178/badge)](https://bestpractices.coreinfrastructure.org/projects/4178)

Pravega is an open source distributed storage service implementing **Streams**. It offers Stream as the main primitive for the foundation of reliable storage systems: a *high-performance, durable, elastic, and unlimited append-only byte stream with strict ordering and consistency*.

To learn more about Pravega, visit https://pravega.io

## Prerequisites

- Java 11+

 In spite of the requirements of using JDK 11+ to build this project, `client` artifacts (and its dependencies) must be compatible with a *Java 8* runtime. All other components are built and ran using JDK11+.

The `clientJavaVersion` project property determines the version used to build the client (defaults to *8*).
## Building Pravega

Checkout the source code:

```
git clone https://github.com/pravega/pravega.git
cd pravega
```

Build the pravega distribution:

```
./gradlew distribution
```

Install pravega jar files into the local maven repository. This is handy for running the `pravega-samples` locally against a custom version of pravega.

```
./gradlew install
```

Running unit tests:

```
./gradlew test
```

## Setting up your IDE

Pravega uses [Project Lombok](https://projectlombok.org/) so you should ensure you have your IDE setup with the required plugins. Using IntelliJ is recommended.

To import the source into IntelliJ:

1. Import the project directory into IntelliJ IDE. It will automatically detect the gradle project and import things correctly.
2. Enable `Annotation Processing` by going to `Build, Execution, Deployment` -> `Compiler` > `Annotation Processors` and checking 'Enable annotation processing'.
3. Install the `Lombok Plugin`. This can be found in `Preferences` -> `Plugins`. Restart your IDE.
4. Pravega should now compile properly.

For eclipse, you can generate eclipse project files by running `./gradlew eclipse`.

<sup> Note: Some unit tests will create (and delete) a significant amount of files. For improved performance on Windows machines, be sure to add the appropriate 'Microsoft Defender' exclusion.</sup>

## Releases

The latest pravega releases can be found on the [Github Release](https://github.com/pravega/pravega/releases) project page.

## Snapshot artifacts 

All snapshot artifacts from `master` and `release` branches are available in GitHub Packages Registry

Add the following to your repositories list and import dependencies as usual. 

```
maven {
    url "https://maven.pkg.github.com/pravega/pravega"
    credentials {
        username = "pravega-public"
        password = "\u0067\u0068\u0070\u005F\u0048\u0034\u0046\u0079\u0047\u005A\u0031\u006B\u0056\u0030\u0051\u0070\u006B\u0079\u0058\u006D\u0035\u0063\u0034\u0055\u0033\u006E\u0032\u0065\u0078\u0039\u0032\u0046\u006E\u0071\u0033\u0053\u0046\u0076\u005A\u0049"
    }
}
```
Note GitHub Packages requires authentication to download packages thus credentials above are required. Use the provided password as is, please do not decode it. 

If you need a dedicated token to use in your repository (and GitHub Actions) please reach out to us.  

As alternative option you can use JitPack (https://jitpack.io/#pravega/pravega) to get pre-release artifacts. 

## Quick Start

Read [Getting Started](documentation/src/docs/getting-started/quick-start.md) page for more information, and also visit [sample-apps](https://github.com/pravega/pravega-samples) repo for more applications.

## Running Pravega

Pravega can be installed locally or in a distributed environment. The installation and deployment of pravega is covered in the [Running Pravega](documentation/src/docs/deployment/deployment.md) guide.

## Support

Don’t hesitate to ask! Contact the developers and community on [slack](https://pravega-io.slack.com/) 
([signup](https://join.slack.com/t/pravega-io/shared_invite/zt-245sgpw47-vbLBLiLfBdW9TlKemXkUnw)) if you need any help. 
Open an issue if you found a bug on [Github Issues](https://github.com/pravega/pravega/issues).

## Documentation

The Pravega documentation is hosted on the website:
<https://pravega.io/docs/latest> or in the [documentation](documentation/src/docs) directory of the source code.

## Contributing

Become one of the contributors! We thrive to build a welcoming and open
community for anyone who wants to use the system or contribute to it.
[Here](documentation/src/docs/contributing.md) we describe how to contribute to Pravega!
You can see the roadmap document [here](documentation/src/docs/roadmap.md).

## About

Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
