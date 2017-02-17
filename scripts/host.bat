@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  host startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Add default JVM options here. You can also use JAVA_OPTS and HOST_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS="-server" "-Xms512m" "-XX:+HeapDumpOnOutOfMemoryError"

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto init

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:init
@rem Get command-line arguments, handling Windows variants

if not "%OS%" == "Windows_NT" goto win9xME_args

:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\host.jar;%APP_HOME%\lib\logback-classic-1.1.7.jar;%APP_HOME%\lib\commons-lang-2.6.jar;%APP_HOME%\lib\lombok-1.16.10.jar;%APP_HOME%\lib\commons-io-2.5.jar;%APP_HOME%\lib\netty-all-4.0.36.Final.jar;%APP_HOME%\lib\common.jar;%APP_HOME%\lib\contracts.jar;%APP_HOME%\lib\storage.jar;%APP_HOME%\lib\impl.jar;%APP_HOME%\lib\server.jar;%APP_HOME%\lib\logback-core-1.1.7.jar;%APP_HOME%\lib\curator-recipes-2.11.0.jar;%APP_HOME%\lib\metrics3-statsd-4.1.0.jar;%APP_HOME%\lib\metrics-core-3.1.0.jar;%APP_HOME%\lib\metrics-jvm-3.1.0.jar;%APP_HOME%\lib\distributedlog-core-0.3.51-RC1.jar;%APP_HOME%\lib\distributedlog-client-0.3.51-RC1.jar;%APP_HOME%\lib\rocksdbjni-4.11.2.jar;%APP_HOME%\lib\hadoop-common-2.7.3.jar;%APP_HOME%\lib\hadoop-hdfs-2.7.3.jar;%APP_HOME%\lib\curator-framework-2.11.0.jar;%APP_HOME%\lib\metrics-statsd-common-4.1.0.jar;%APP_HOME%\lib\zookeeper-3.5.1-alpha.jar;%APP_HOME%\lib\stats-util-0.0.58.jar;%APP_HOME%\lib\util-core_2.11-6.34.0.jar;%APP_HOME%\lib\commons-lang3-3.3.2.jar;%APP_HOME%\lib\jline-0.9.94.jar;%APP_HOME%\lib\libthrift-0.5.0-1.jar;%APP_HOME%\lib\scrooge-core_2.11-4.6.0.jar;%APP_HOME%\lib\bookkeeper-server-4.3.4-TWTTR.jar;%APP_HOME%\lib\distributedlog-protocol-0.3.51-RC1.jar;%APP_HOME%\lib\lz4-1.2.0.jar;%APP_HOME%\lib\finagle-core_2.11-6.34.0.jar;%APP_HOME%\lib\finagle-thriftmux_2.11-6.34.0.jar;%APP_HOME%\lib\finagle-serversets_2.11-6.34.0.jar;%APP_HOME%\lib\hadoop-annotations-2.7.3.jar;%APP_HOME%\lib\commons-math3-3.1.1.jar;%APP_HOME%\lib\xmlenc-0.52.jar;%APP_HOME%\lib\commons-httpclient-3.1.jar;%APP_HOME%\lib\commons-net-3.1.jar;%APP_HOME%\lib\commons-collections-3.2.2.jar;%APP_HOME%\lib\servlet-api-2.5.jar;%APP_HOME%\lib\jetty-6.1.26.jar;%APP_HOME%\lib\jetty-util-6.1.26.jar;%APP_HOME%\lib\jsp-api-2.1.jar;%APP_HOME%\lib\jersey-core-1.9.jar;%APP_HOME%\lib\jersey-json-1.9.jar;%APP_HOME%\lib\jersey-server-1.9.jar;%APP_HOME%\lib\commons-logging-1.1.3.jar;%APP_HOME%\lib\log4j-1.2.17.jar;%APP_HOME%\lib\jets3t-0.9.0.jar;%APP_HOME%\lib\commons-configuration-1.6.jar;%APP_HOME%\lib\slf4j-log4j12-1.7.10.jar;%APP_HOME%\lib\jackson-core-asl-1.9.13.jar;%APP_HOME%\lib\jackson-mapper-asl-1.9.13.jar;%APP_HOME%\lib\avro-1.7.4.jar;%APP_HOME%\lib\protobuf-java-2.5.0.jar;%APP_HOME%\lib\gson-2.2.4.jar;%APP_HOME%\lib\hadoop-auth-2.7.3.jar;%APP_HOME%\lib\jsch-0.1.42.jar;%APP_HOME%\lib\htrace-core-3.1.0-incubating.jar;%APP_HOME%\lib\commons-compress-1.4.1.jar;%APP_HOME%\lib\commons-daemon-1.0.13.jar;%APP_HOME%\lib\xercesImpl-2.9.1.jar;%APP_HOME%\lib\leveldbjni-all-1.8.jar;%APP_HOME%\lib\util-0.0.120.jar;%APP_HOME%\lib\guice-3.0.jar;%APP_HOME%\lib\scala-library-2.11.7.jar;%APP_HOME%\lib\util-function_2.11-6.34.0.jar;%APP_HOME%\lib\jsr166e-1.0.0.jar;%APP_HOME%\lib\scala-parser-combinators_2.11-1.0.4.jar;%APP_HOME%\lib\junit-3.8.1.jar;%APP_HOME%\lib\bookkeeper-stats-api-4.3.4-TWTTR.jar;%APP_HOME%\lib\jna-3.2.7.jar;%APP_HOME%\lib\finagle-thrift_2.11-6.34.0.jar;%APP_HOME%\lib\util-app_2.11-6.33.0.jar;%APP_HOME%\lib\util-cache_2.11-6.33.0.jar;%APP_HOME%\lib\util-codec_2.11-6.33.0.jar;%APP_HOME%\lib\util-collection_2.11-6.33.0.jar;%APP_HOME%\lib\util-hashing_2.11-6.33.0.jar;%APP_HOME%\lib\util-jvm_2.11-6.33.0.jar;%APP_HOME%\lib\util-lint_2.11-6.33.0.jar;%APP_HOME%\lib\util-logging_2.11-6.33.0.jar;%APP_HOME%\lib\util-registry_2.11-6.33.0.jar;%APP_HOME%\lib\util-stats_2.11-6.33.0.jar;%APP_HOME%\lib\finagle-mux_2.11-6.34.0.jar;%APP_HOME%\lib\server-set-1.0.103.jar;%APP_HOME%\lib\util-zk-common_2.11-6.33.0.jar;%APP_HOME%\lib\jackson-core-2.4.4.jar;%APP_HOME%\lib\jackson-databind-2.4.4.jar;%APP_HOME%\lib\jackson-module-scala_2.11-2.4.4.jar;%APP_HOME%\lib\jettison-1.1.jar;%APP_HOME%\lib\jaxb-impl-2.2.3-1.jar;%APP_HOME%\lib\jackson-jaxrs-1.8.3.jar;%APP_HOME%\lib\jackson-xc-1.8.3.jar;%APP_HOME%\lib\asm-3.1.jar;%APP_HOME%\lib\java-xmlbuilder-0.4.jar;%APP_HOME%\lib\commons-digester-1.8.jar;%APP_HOME%\lib\commons-beanutils-core-1.8.0.jar;%APP_HOME%\lib\snappy-java-1.0.4.1.jar;%APP_HOME%\lib\apacheds-kerberos-codec-2.0.0-M15.jar;%APP_HOME%\lib\xz-1.0.jar;%APP_HOME%\lib\util-executor-service-shutdown-0.0.66.jar;%APP_HOME%\lib\util-system-mocks-0.0.103.jar;%APP_HOME%\lib\jdk-logging-0.0.79.jar;%APP_HOME%\lib\base-0.0.114.jar;%APP_HOME%\lib\collections-0.0.109.jar;%APP_HOME%\lib\quantity-0.0.98.jar;%APP_HOME%\lib\stats-0.0.114.jar;%APP_HOME%\lib\javax.inject-1.jar;%APP_HOME%\lib\aopalliance-1.0.jar;%APP_HOME%\lib\cglib-2.2.1-v20090111.jar;%APP_HOME%\lib\args-0.2.35.jar;%APP_HOME%\lib\io-0.0.65.jar;%APP_HOME%\lib\io-json-0.0.51.jar;%APP_HOME%\lib\io-thrift-0.0.64.jar;%APP_HOME%\lib\dynamic-host-set-0.0.53.jar;%APP_HOME%\lib\service-thrift-1.0.54.jar;%APP_HOME%\lib\util-zk_2.11-6.33.0.jar;%APP_HOME%\lib\scala-reflect-2.11.2.jar;%APP_HOME%\lib\jaxb-api-2.2.2.jar;%APP_HOME%\lib\commons-beanutils-1.7.0.jar;%APP_HOME%\lib\apacheds-i18n-2.0.0-M15.jar;%APP_HOME%\lib\api-asn1-api-1.0.0-M20.jar;%APP_HOME%\lib\api-util-1.0.0-M20.jar;%APP_HOME%\lib\args-apt-0.1.33.jar;%APP_HOME%\lib\args-core-0.1.33.jar;%APP_HOME%\lib\stax-api-1.0-2.jar;%APP_HOME%\lib\activation-1.1.jar;%APP_HOME%\lib\slf4j-api-1.7.20.jar;%APP_HOME%\lib\guava-16.0.1.jar;%APP_HOME%\lib\commons-cli-1.2.jar;%APP_HOME%\lib\jsr305-3.0.0.jar;%APP_HOME%\lib\javacc-5.0.jar;%APP_HOME%\lib\netty-3.10.1.Final.jar;%APP_HOME%\lib\commons-codec-1.9.jar;%APP_HOME%\lib\httpclient-4.2.5.jar;%APP_HOME%\lib\stat-registry-0.0.57.jar;%APP_HOME%\lib\stats-provider-0.0.92.jar;%APP_HOME%\lib\application-action-0.0.89.jar;%APP_HOME%\lib\util-sampler-0.0.77.jar;%APP_HOME%\lib\stat-0.0.72.jar;%APP_HOME%\lib\client-0.0.79.jar;%APP_HOME%\lib\group-0.0.90.jar;%APP_HOME%\lib\jackson-annotations-2.4.4.jar;%APP_HOME%\lib\paranamer-2.6.jar;%APP_HOME%\lib\httpcore-4.2.4.jar;%APP_HOME%\lib\curator-client-2.11.0.jar;%APP_HOME%\lib\net-util-0.0.100.jar

@rem Execute host
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %HOST_OPTS%  -classpath "%CLASSPATH%" com.emc.pravega.service.server.host.ServiceStarter %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

:fail
rem Set variable HOST_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%HOST_EXIT_CONSOLE%" exit 1
exit /b 1

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
