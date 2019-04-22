# Running System Tests with Security Enabled

## Objective
The objective of running system tests with Security(Auth+TLS) enabled, is to very that all Pravega functionality functions as expected in a distributed cluster with Security enabled.
This is not for testing working of specific security features like access control, encryption etc... as that should get covered under Unit Tests. However, a few basic security specific verifications could be added like scope creation failing when authenication fails.
[Role of Security System Tests](https://asdwiki.isus.emc.com:8443/pages/viewpage.action?spaceKey=~shardr&title=Pravega+Security+System+Test+Cases)

## Cluster Setup for System Tests
The default cluster setup, for system tests has security disabled. (security related system properties are not set)
For running tests with Security enabled, compponents like the controller, segment store, Zk and Bookeeper need to be started with authentication and TLS enabled, by setting appropriate system properties at startup. As such, Security System Tests cannot be run on the same cluster as regular system tests.
Also, Pravega Client in the test case needs to be created with the appropriate authentication and TLS parameters.

## Running tests with Security enabled.
To enable security on a set of components, certain system properties need to be set on the pravega operator and the components need to be started with those properties set. All system properties to be set for a Pravega cluster will be encapsulated in a 'PravegaProperties' Object and set on the Operator to create a cluster with security features enabled/disabled.

## Approaches Evaluated

The following approaches were considered for running System Tests with & without Security:

|SNo.|Approach|Description|Pros|Cons|Notes|
|:-:|:---|:---------|:--------------|:---------------|:------|
|1|Setup and Tear down after each test class|Requires Parameterized JUnit tests, parameter - PravegaContext (PravegaProperties object + Client object). Tear down method to run after execution of all tests in a class for single parameter value.|Clean environment for each test. |Increased execution time. No JUnit support for tear down after each parameter execution.|Discarded|
|2| Setup 2 different clusters | Using test framework code, Set-up 2 clusters, one with security enabled and another without security enabled. Requires Parameterized JUnit tests, for running same test with/without security.|No tear down needed.|Difficult to code, maintain and understand. Operator support?? Hardware should have capacity to spin up 2 clusters and run tests without any issues.|Discarded|
|3|New gradle task for security system tests|Two new gradle tasks 'startK8SecuritySystemTests':runs system tests with security enabled. 'startAllSystemTests':Invoke 'startK8SystemTests' followed by cluster cleanup and then 'startK8SecuritySystemTests'. An environment variable to determine if 'startK8SecuritySystemTests' should run or not. initialize()(@Environment) method in tests to create cluster with/without Security based on value of a specific system property. By default, execution of 'startK8SecuritySystemTests' could be disabled since in many cases running the same tests with Security enabled would not be necessary. Currently some tests fail when Auth is enabled, till these are fixed it would be to good to not have security system tests run as part of the regular system test suite.|Simplicity, low cost(one cluster can run both sets of tests, minimum time spent in cluster setup/teardown)|None|Selected|
|4|Run Security System Tests as a separate Jenkins build| Same as 3 except that there will be no additional gradle tasks. The task 'startK8SysteTests' will by default run tests without Security. To run them with Security a specific property needs to be set and tests invoked using a separate Jenkins build|Simple|Security tests should not be a separate jenkins build|Discarded|




 
