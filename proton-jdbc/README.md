# Proton JDBC driver

Build on top of `proton-client`, `proton-jdbc` follows JDBC standards and provides additional features like custom type mapping, fake transaction, and standard synchronous UPDATE and DELETE statement etc., so that it can be easily used together with legacy applications and tools.

Keep in mind that `proton-jdbc` is synchronous, and in general it has more overheads(e.g. SQL parsing and type mapping/conversion etc.). You should consider `proton-client` when performance is critical and/or you prefer more direct way to work with Proton.

## Maven Dependency
This library is available on maven central repository since Nov 17, 2023.
 
```xml
<dependency>
    <groupId>com.timeplus</groupId>
    <artifactId>proton-jdbc</artifactId>
    <version>0.6.0</version>
</dependency>
```

## Gradle
```
dependencies {
    implementation 'com.timeplus:proton-jdbc:0.6.0'
}
```

## Configuration

* Driver Class: `com.timeplus.proton.jdbc.ProtonDriver`
* JDBC URL: `jdbc:proton://localhost:8123` or `jdbc:proton://localhost:8123/default`
* Username is `default` and password is an empty string

## More Documents and Examples

- [How to connect to Proton via JDBC](https://github.com/timeplus-io/proton/tree/develop/examples/jdbc)
- [examples](https://github.com/timeplus-io/proton-java-driver/tree/develop/examples/jdbc)

## How to build from sources

Recommend to use Java 11 and Maven. For example, on Mac OS X

```bash
sdk install java 11.0.21-tem
brew install maven
```

First clone the repository:

```bash
git clone https://github.com/timeplus-io/proton-java-driver.git
cd proton-java-driver
```

To succesfully built the jar and pass the tests, you need to start Proton server with 3218 port open. You can use [this docker compose file](https://github.com/timeplus-io/proton/tree/develop/examples/jdbc).

Then, run the command in terminal:

```bash
cd third-party-libraries
mvn -Drelease clean install
cd ../proton-client
mvn -Drelease clean install
cd ../proton-http-client
mvn -Drelease clean install
cd ../proton-jdbc
mvn -Drelease clean source:jar install verify
```

After that, proton-jdbc will be installed in your local maven repository. You can also get the JAR files in proton-jdbc/target. `proton-jdbc-<version>-all.jar` is recommended to add to your JDBC client or project. But the better way is to load the jar from maven: `com.timeplus:proton-jdbc:0.6.0`.

To upload the new version to maven repository, run the above commands to generate JAR files. In the deployment subfolder, update the pom file. Also copy javadoc.jar, sources.jar with their md5 and sha1. You also need to copy proton-jdbc-version-all.jar and its md5 and sha1 but remove the `-all` from the file names. Create md5 and sha1 files for the pom file, using `md5` and `shasum` commands. Create the .asc files with `gpg -ab <filename>` commpands. You need to get the passcode from Jove. Finally go to the deployment folder and run `zip -r proton-jdbc-<version> ./`. In https://central.sonatype.com/publishing page, upload the deployment zip file. Wait for 3 minutes to verify it and 30 minutes to publish it.