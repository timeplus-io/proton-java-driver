## Proton JDBC driver

This repo is a forked from https://github.com/ClickHouse/clickhouse-java with necessary revisions to better fit streaming processing and Proton.
The build / packaging process etc are the same as the source repo.

This library is available on maven central repository since Nov 17, 2023.
 
```xml
<dependency>
    <groupId>com.timeplus</groupId>
    <artifactId>proton-jdbc</artifactId>
    <version>0.5.0</version>
</dependency>
```

```
dependencies {
    implementation 'com.timeplus:proton-jdbc:0.5.0'
}
```

For how to use the JDBC driver, please check the [README in proton repo](https://github.com/timeplus-io/proton/tree/develop/examples/jdbc).

For how to compile the JDBC driver, please check [proton-jdbc](proton-jdbc) sub-folder.