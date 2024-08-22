## Timeplus JDBC driver (over HTTP)

This repo is a forked from https://github.com/ClickHouse/clickhouse-java with necessary revisions to better fit streaming processing and Timeplus. It works for both [Timeplus Proton](https://github.com/timeplus-io/proton) and [Timeplus Enterprise](https://docs.timeplus.com/timeplus-enterprise).
The build / packaging process etc are the same as the source repo.

This library is available on maven central repository since Nov 17, 2023.
 
```xml
<dependency>
    <groupId>com.timeplus</groupId>
    <artifactId>proton-jdbc</artifactId>
    <version>${http-jdbc.version}</version>
</dependency>
```

```
dependencies {
    implementation 'com.timeplus:proton-jdbc:${http-jdbc.version}'
}
```

For how to use the JDBC driver, please check the [README in proton repo](https://github.com/timeplus-io/proton/tree/develop/examples/jdbc).

To compile and publish the project, run the following command (making sure you have a Timeplus Proton or Enterprise running):
```bash
export GPG_TTY=$(tty)
mvn -Drelease clean source:jar package install gpg:sign -Drevision=<version>
# or disable tests
mvn -Drelease clean source:jar package install gpg:sign -DskipITs -DskipTests -Drevision=..
```

In some cases,the asc file is not properly generated and cannot be verified via `gpg --verify proton-jdbc/target/proton-jdbc-<version>.jar.asc` You may need to regenerate it via `gpg -ab proton-jdbc/target/proton-jdbc-<version>.jar`

Please check the README in [proton-jdbc](proton-jdbc) sub-folder for more details.