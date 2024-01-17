@rem
@rem Copyright 2015 the original author or authors.
@rem
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem You may obtain a copy of the License at
@rem
@rem      https://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem

@if "%DEBUG%"=="" @echo off
@rem ##########################################################################
@rem
@rem  maj_orm startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%"=="" set DIRNAME=.
@rem This is normally unused
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

@rem Add default JVM options here. You can also use JAVA_OPTS and MAJ_ORM_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if %ERRORLEVEL% equ 0 goto execute

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto execute

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\maj_orm-1.0-SNAPSHOT-plain.jar;%APP_HOME%\lib\sitebridge.jar;%APP_HOME%\lib\aws-java-sdk-s3-1.12.268.jar;%APP_HOME%\lib\aws-java-sdk-kms-1.12.268.jar;%APP_HOME%\lib\aws-java-sdk-core-1.12.268.jar;%APP_HOME%\lib\httpclient-4.5.7.jar;%APP_HOME%\lib\commons-dbutils-1.8.1.jar;%APP_HOME%\lib\spring-jdbc-6.1.1.jar;%APP_HOME%\lib\spring-data-cassandra-4.1.3.jar;%APP_HOME%\lib\spring-tx-6.1.1.jar;%APP_HOME%\lib\spring-boot-starter-actuator-3.2.0.jar;%APP_HOME%\lib\jakarta.persistence-api-3.1.0.jar;%APP_HOME%\lib\java-driver-mapper-processor-4.17.0.jar;%APP_HOME%\lib\java-driver-mapper-runtime-4.17.0.jar;%APP_HOME%\lib\java-driver-query-builder-4.17.0.jar;%APP_HOME%\lib\java-driver-core-4.17.0.jar;%APP_HOME%\lib\HikariCP-5.0.0.jar;%APP_HOME%\lib\je-4.0.92.jar;%APP_HOME%\lib\jedis-4.4.6.jar;%APP_HOME%\lib\commons-text-1.10.0.jar;%APP_HOME%\lib\commons-lang3-3.5.jar;%APP_HOME%\lib\javafx-base-18.jar;%APP_HOME%\lib\mysql-connector-j-8.1.0.jar;%APP_HOME%\lib\tomcat-embed-core-9.0.65.jar;%APP_HOME%\lib\commons-fileupload-1.4.jar;%APP_HOME%\lib\mssql-jdbc-9.4.1.jre11.jar;%APP_HOME%\lib\javers-core-5.6.3.jar;%APP_HOME%\lib\guava-31.1-jre.jar;%APP_HOME%\lib\spring-boot-actuator-autoconfigure-3.0.5.jar;%APP_HOME%\lib\jackson-datatype-jsr310-2.13.2.jar;%APP_HOME%\lib\jmespath-java-1.12.268.jar;%APP_HOME%\lib\jackson-databind-2.13.2.jar;%APP_HOME%\lib\jackson-annotations-2.13.2.jar;%APP_HOME%\lib\jackson-dataformat-cbor-2.14.2.jar;%APP_HOME%\lib\jackson-core-2.13.2.jar;%APP_HOME%\lib\super-csv-2.4.0.jar;%APP_HOME%\lib\json-simple-1.1.1.jar;%APP_HOME%\lib\javax.mail-1.5.5.jar;%APP_HOME%\lib\mockito-core-2.1.0.jar;%APP_HOME%\lib\shadow-7.1.2.jar;%APP_HOME%\lib\commons-io-2.11.0.jar;%APP_HOME%\lib\zip4j-2.11.2.jar;%APP_HOME%\lib\jsoup-1.15.3.jar;%APP_HOME%\lib\commons-csv-1.9.0.jar;%APP_HOME%\lib\threeten-extra-1.7.1.jar;%APP_HOME%\lib\httpcore-4.4.16.jar;%APP_HOME%\lib\commons-logging-1.2.jar;%APP_HOME%\lib\commons-codec-1.15.jar;%APP_HOME%\lib\spring-boot-starter-3.0.5.jar;%APP_HOME%\lib\spring-boot-autoconfigure-3.0.5.jar;%APP_HOME%\lib\spring-boot-actuator-3.0.5.jar;%APP_HOME%\lib\spring-boot-3.0.5.jar;%APP_HOME%\lib\spring-context-6.0.7.jar;%APP_HOME%\lib\spring-data-commons-3.0.4.jar;%APP_HOME%\lib\spring-aop-6.0.7.jar;%APP_HOME%\lib\spring-beans-6.0.7.jar;%APP_HOME%\lib\spring-expression-6.0.7.jar;%APP_HOME%\lib\spring-core-6.0.7.jar;%APP_HOME%\lib\micrometer-jakarta9-1.12.0.jar;%APP_HOME%\lib\micrometer-core-1.10.5.jar;%APP_HOME%\lib\micrometer-observation-1.10.5.jar;%APP_HOME%\lib\native-protocol-1.5.1.jar;%APP_HOME%\lib\netty-handler-4.1.90.Final.jar;%APP_HOME%\lib\java-driver-shaded-guava-25.1-jre-graal-sub-1.jar;%APP_HOME%\lib\config-1.4.1.jar;%APP_HOME%\lib\jnr-posix-3.1.15.jar;%APP_HOME%\lib\metrics-core-4.2.18.jar;%APP_HOME%\lib\spring-boot-starter-logging-3.0.5.jar;%APP_HOME%\lib\logback-classic-1.4.6.jar;%APP_HOME%\lib\log4j-to-slf4j-2.19.0.jar;%APP_HOME%\lib\jul-to-slf4j-2.0.7.jar;%APP_HOME%\lib\slf4j-api-2.0.7.jar;%APP_HOME%\lib\HdrHistogram-2.1.12.jar;%APP_HOME%\lib\reactive-streams-1.0.4.jar;%APP_HOME%\lib\jcip-annotations-1.0-1.jar;%APP_HOME%\lib\spotbugs-annotations-3.1.12.jar;%APP_HOME%\lib\javapoet-1.13.0.jar;%APP_HOME%\lib\commons-pool2-2.11.1.jar;%APP_HOME%\lib\json-20231013.jar;%APP_HOME%\lib\gson-2.9.1.jar;%APP_HOME%\lib\tomcat-annotations-api-10.1.7.jar;%APP_HOME%\lib\failureaccess-1.0.1.jar;%APP_HOME%\lib\listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar;%APP_HOME%\lib\jsr305-3.0.2.jar;%APP_HOME%\lib\checker-qual-3.12.0.jar;%APP_HOME%\lib\error_prone_annotations-2.11.0.jar;%APP_HOME%\lib\j2objc-annotations-1.3.jar;%APP_HOME%\lib\junit-4.13.2.jar;%APP_HOME%\lib\activation-1.1.jar;%APP_HOME%\lib\byte-buddy-1.12.23.jar;%APP_HOME%\lib\byte-buddy-agent-1.12.23.jar;%APP_HOME%\lib\objenesis-2.4.jar;%APP_HOME%\lib\picocontainer-2.15.jar;%APP_HOME%\lib\classgraph-4.8.43.jar;%APP_HOME%\lib\joda-time-2.9.7.jar;%APP_HOME%\lib\spring-jcl-6.0.7.jar;%APP_HOME%\lib\jakarta.annotation-api-2.1.1.jar;%APP_HOME%\lib\snakeyaml-1.33.jar;%APP_HOME%\lib\micrometer-commons-1.10.5.jar;%APP_HOME%\lib\netty-transport-native-unix-common-4.1.90.Final.jar;%APP_HOME%\lib\netty-codec-4.1.90.Final.jar;%APP_HOME%\lib\netty-transport-4.1.90.Final.jar;%APP_HOME%\lib\netty-resolver-4.1.90.Final.jar;%APP_HOME%\lib\netty-buffer-4.1.90.Final.jar;%APP_HOME%\lib\netty-common-4.1.90.Final.jar;%APP_HOME%\lib\jnr-ffi-2.2.11.jar;%APP_HOME%\lib\jnr-constants-0.10.3.jar;%APP_HOME%\lib\hamcrest-core-2.2.jar;%APP_HOME%\lib\ion-java-1.0.2.jar;%APP_HOME%\lib\jdom2-2.0.6.1.jar;%APP_HOME%\lib\asm-commons-9.2.jar;%APP_HOME%\lib\asm-util-9.2.jar;%APP_HOME%\lib\asm-analysis-9.2.jar;%APP_HOME%\lib\asm-tree-9.2.jar;%APP_HOME%\lib\asm-9.2.jar;%APP_HOME%\lib\ant-1.10.11.jar;%APP_HOME%\lib\plexus-utils-3.4.1.jar;%APP_HOME%\lib\log4j-core-2.19.0.jar;%APP_HOME%\lib\jdependency-2.7.0.jar;%APP_HOME%\lib\LatencyUtils-2.0.3.jar;%APP_HOME%\lib\jffi-1.3.9.jar;%APP_HOME%\lib\jffi-1.3.9-native.jar;%APP_HOME%\lib\jnr-a64asm-1.0.0.jar;%APP_HOME%\lib\jnr-x86asm-1.0.2.jar;%APP_HOME%\lib\hamcrest-2.2.jar;%APP_HOME%\lib\ant-launcher-1.10.11.jar;%APP_HOME%\lib\log4j-api-2.19.0.jar;%APP_HOME%\lib\logback-core-1.4.6.jar


@rem Execute maj_orm
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %MAJ_ORM_OPTS%  -classpath "%CLASSPATH%" com.majorana.orm.Maim %*

:end
@rem End local scope for the variables with windows NT shell
if %ERRORLEVEL% equ 0 goto mainEnd

:fail
rem Set variable MAJ_ORM_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
set EXIT_CODE=%ERRORLEVEL%
if %EXIT_CODE% equ 0 set EXIT_CODE=1
if not ""=="%MAJ_ORM_EXIT_CONSOLE%" exit %EXIT_CODE%
exit /b %EXIT_CODE%

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
