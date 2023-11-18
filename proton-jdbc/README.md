# Proton JDBC driver

Build on top of `proton-client`, `proton-jdbc` follows JDBC standards and provides additional features like custom type mapping, fake transaction, and standard synchronous UPDATE and DELETE statement etc., so that it can be easily used together with legacy applications and tools.

Keep in mind that `proton-jdbc` is synchronous, and in general it has more overheads(e.g. SQL parsing and type mapping/conversion etc.). You should consider `proton-client` when performance is critical and/or you prefer more direct way to work with Proton.

## Maven Dependency

```xml
<dependency>
    <groupId>com.timeplus</groupId>
    <artifactId>proton-jdbc</artifactId>
    <version>0.4.0</version>
</dependency>
```

## Configuration

**Driver Class**: `com.timeplus.proton.jdbc.ProtonDriver`

**URL Syntax**: `jdbc:<prefix>[:<protocol>]://<host>:[<port>][/<database>[?param1=value1&param2=value2]]`, for examples:

- `jdbc:ch:grpc://localhost` is same as `jdbc:proton:grpc://localhost:9100`
- `jdbc:ch:grpc://localhost` is same as `jdbc:proton:grpc://localhost:9100`)
- `jdbc:ch://localhost/test?socket_timeout=120000`

**Connection Properties**:

| Property                 | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------ | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| continueBatchOnError     | `false` | Whether to continue batch processing when error occurred                                                                                                                                                                                                                                                                                                                                                                   |
| createDatabaseIfNotExist | `false` | Whether to create database if it does not exist                                                                                                                                                                                                                                                                                                                                                                            |
| custom_http_headers      |         | comma separated custom http headers, for example: `User-Agent=client1,X-Gateway-Id=123`                                                                                                                                                                                                                                                                                                                                    |
| custom_http_params       |         | comma separated custom http query parameters, for example: `extremes=0,max_result_rows=100`                                                                                                                                                                                                                                                                                                                                |
| jdbcCompliance           | `true`  | Whether to support standard synchronous UPDATE/DELETE and fake transaction                                                                                                                                                                                                                                                                                                                                                 |
| typeMappings             |         | Customize mapping between Proton data type and Java class, which will affect result of both [getColumnType()](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSetMetaData.html#getColumnType-int-) and [getObject(Class<?>)](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html#getObject-java.lang.String-java.lang.Class-). For example: `UInt128=java.lang.String,UInt256=java.lang.String` |
| wrapperObject            | `false` | Whether [getObject()](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html#getObject-int-) should return java.sql.Array / java.sql.Struct for Array / Tuple.                                                                                                                                                                                                                                                  |

Note: please refer to [JDBC specific configuration](https://github.com/timeplus-io/proton-java-driver/blob/master/proton-jdbc/src/main/java/com/proton/jdbc/JdbcConfig.java) and client options([common](https://github.com/timeplus-io/proton-java-driver/blob/master/proton-client/src/main/java/com/proton/client/config/ProtonClientOption.java), [http](https://github.com/timeplus-io/proton-java-driver/blob/master/proton-http-client/src/main/java/com/proton/client/http/config/ProtonHttpOption.java) and [grpc](https://github.com/timeplus-io/proton-java-driver/blob/master/proton-grpc-client/src/main/java/com/proton/client/grpc/config/ProtonGrpcOption.java)) for more.

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

Then, run the command in terminal:

```bash
cd third-party-libraries
mvn -Drelease clean install
cd ../proton-client
mvn -Drelease clean install
cd ../proton-http-client
mvn -Drelease clean install
cd ../proton-jdbc
mvn -Drelease clean install
```

After that, proton-jdbc will be installed in your local maven repository. You can also get the JAR files in proton-jdbc/target. `proton-jdbc-<version>-all.jar` is recommended to add to your JDBC client or project.