# Proton JDBC driver

Build on top of `proton-client`, `proton-jdbc` follows JDBC standards and provides additional features like custom type mapping, fake transaction, and standard synchronous UPDATE and DELETE statement etc., so that it can be easily used together with legacy applications and tools.

Keep in mind that `proton-jdbc` is synchronous, and in general it has more overheads(e.g. SQL parsing and type mapping/conversion etc.). You should consider `proton-client` when performance is critical and/or you prefer more direct way to work with Proton.

## Maven Dependency

```xml
<dependency>
    <!-- will stop using ru.yandex.proton starting from 0.4.0 -->
    <groupId>com.proton</groupId>
    <artifactId>proton-jdbc</artifactId>
    <version>0.3.2-patch7</version>
</dependency>
```

## Configuration

**Driver Class**: `com.proton.jdbc.ProtonDriver`

Note: `ru.yandex.proton.ProtonDriver` and everything under `ru.yandex.proton` will be removed starting from 0.4.0.

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

## Examples

<details>
    <summary>Connect to Proton and issue a query...</summary>

```java
String url = "jdbc:ch://my-server/system"; // use http protocol and port 8123 by default
// String url = "jdbc:ch://my-server:8443/system"; // if you prefer https
Properties properties = new Properties();
// properties.setProperty("ssl", "true");
// properties.setProperty("sslmode", "NONE"); // NONE to trust all servers; STRICT for trusted only
ProtonDataSource dataSource = new ProtonDataSource(url, new Properties());
try (Connection conn = dataSource.getConnection("default", "password");
    Statement stmt = conn.createStatement()) {
    ResultSet rs = stmt.executeQuery("select * from numbers(50000)");
    while(rs.next()) {
        // ...
    }
}
```

</details>

<details>
    <summary>Batch insert...</summary>

Tips:

1. Use `PreparedStatement` instead of `Statement`
2. Use [input function](https://proton.com/docs/en/sql-reference/table-functions/input/) whenever possible

```java
// create table mytable(id String, timestamp DateTime64(3), description Nullable(String)) engine=Memory

// 1. recommended as it performs the best
try (PreparedStatement ps = conn.prepareStatement(
    "insert into mytable select col1, col2 from input('col1 String, col2 DateTime64(3), col3 Int32')")) {
    // the column definition will be parsed so the driver knows there are 3 parameters: col1, col2 and col3
    ps.setString(1, "test"); // col1
    ps.setObject(2, LocalDateTime.now()); // col2, setTimestamp is slow and not recommended
    ps.setInt(3, 123); // col3
    ps.addBatch(); // parameters will be write into buffered stream immediately in binary format
    ...
    ps.executeBatch(); // stream everything on-hand into Proton
}

// 2. easier to use but slower compare to input function
try (PreparedStatement ps = conn.prepareStatement("insert into mytable(* except (desc))")) {
    // the driver will issue query "select * except (description) from mytable where 0" for type inferring
    // since description column is excluded, we know there are only two parameters: col1 and col2
    ps.setString(1, "test"); // id
    ps.setObject(2, LocalDateTime.now()); // timestamp
    ps.addBatch(); // parameters will be write into buffered stream immediately in binary format
    ...
    ps.executeBatch(); // stream everything on-hand into Proton
}

// 3. not recommended as it's based on a large SQL
// Note: "insert into mytable values(?,?,?)" is treated as "insert into mytable"
try (PreparedStatement ps = conn.prepareStatement("insert into mytable values(trim(?),?,?)")) {
    ps.setString(1, "test"); // id
    ps.setObject(2, LocalDateTime.now()); // timestamp
    ps.setString(3, null); // description
    ps.addBatch(); // append parameters to the query
    ...
    ps.executeBatch(); // issue the composed query: insert into mytable values(...)(...)...(...)
}
```

</details>

<details>
    <summary>Handling DateTime and time zone...</summary>

Please to use `java.time.LocalDateTime` or `java.time.OffsetDateTime` instead of `java.sql.Timestamp`, and `java.time.LocalDate` instead of `java.sql.Date`.

```java
try (PreparedStatement ps = conn.prepareStatement("select date_time from mytable where date_time > ?")) {
    ps.setObject(2, LocalDateTime.now());
    ResultSet rs = ps.executeQuery();
    while(rs.next()) {
        LocalDateTime dateTime = (LocalDateTime) rs.getObject(1);
    }
    ...
}
```

</details>

<details>
    <summary>Handling AggregateFunction...</summary>

As of now, only `groupBitmap` is supported.

```java
// batch insert using input function
try (ProtonConnection conn = newConnection(props);
        Statement s = conn.createStatement();
        PreparedStatement stmt = conn.prepareStatement(
                "insert into test_batch_input select id, name, value from input('id Int32, name Nullable(String), desc Nullable(String), value AggregateFunction(groupBitmap, UInt32)')")) {
    s.execute("drop table if exists test_batch_input;"
            + "create table test_batch_input(id Int32, name Nullable(String), value AggregateFunction(groupBitmap, UInt32))engine=Memory");
    Object[][] objs = new Object[][] {
            new Object[] { 1, "a", "aaaaa", ProtonBitmap.wrap(1, 2, 3, 4, 5) },
            new Object[] { 2, "b", null, ProtonBitmap.wrap(6, 7, 8, 9, 10) },
            new Object[] { 3, null, "33333", ProtonBitmap.wrap(11, 12, 13) }
    };
    for (Object[] v : objs) {
        stmt.setInt(1, (int) v[0]);
        stmt.setString(2, (String) v[1]);
        stmt.setString(3, (String) v[2]);
        stmt.setObject(4, v[3]);
        stmt.addBatch();
    }
    int[] results = stmt.executeBatch();
    ...
}

// use bitmap as query parameter
try (PreparedStatement stmt = conn.prepareStatement(
    "SELECT bitmapContains(my_bitmap, toUInt32(1)) as v1, bitmapContains(my_bitmap, toUInt32(2)) as v2 from {tt 'ext_table'}")) {
    stmt.setObject(1, ProtonExternalTable.builder().name("ext_table")
            .columns("my_bitmap AggregateFunction(groupBitmap,UInt32)").format(ProtonFormat.RowBinary)
            .content(new ByteArrayInputStream(ProtonBitmap.wrap(1, 3, 5).toBytes()))
            .asTempTable()
            .build());
    ResultSet rs = stmt.executeQuery();
    Assert.assertTrue(rs.next());
    Assert.assertEquals(rs.getInt(1), 1);
    Assert.assertEquals(rs.getInt(2), 0);
    Assert.assertFalse(rs.next());
}
```

</details>

<details>
    <summary>Before 0.3.2...</summary>

#### **Basic**

```java
String url = "jdbc:proton://localhost:8123/test";
ProtonProperties properties = new ProtonProperties();
// set connection options - see more defined in ProtonConnectionSettings
properties.setClientName("Agent #1");
...
// set default request options - more in ProtonQueryParam
properties.setSessionId("default-session-id");
...

ProtonDataSource dataSource = new ProtonDataSource(url, properties);
String sql = "select * from mytable";
Map<ProtonQueryParam, String> additionalDBParams = new HashMap<>();
// set request options, which will override the default ones in ProtonProperties
additionalDBParams.put(ProtonQueryParam.SESSION_ID, "new-session-id");
...
try (ProtonConnection conn = dataSource.getConnection();
    ProtonStatement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(sql, additionalDBParams)) {
    ...
}
```

Additionally, if you have a few instances, you can use `BalancedProtonDataSource`.

#### **Extended API**

In order to provide non-JDBC complaint data manipulation functionality, proprietary API exists.
Entry point for API is `ProtonStatement#write()` method.

1. Importing file into table

```java
import ru.yandex.proton.ProtonStatement;
ProtonStatement sth = connection.createStatement();
sth
    .write() // Write API entrypoint
    .table("default.my_table") // where to write data
    .option("format_csv_delimiter", ";") // specific param
    .data(new File("/path/to/file.csv.gz"), ProtonFormat.CSV, ProtonCompression.gzip) // specify input
    .send();
```

2. Configurable send

```java
import ru.yandex.proton.ProtonStatement;
ProtonStatement sth = connection.createStatement();
sth
    .write()
    .sql("INSERT INTO default.my_table (a,b,c)")
    .data(new MyCustomInputStream(), ProtonFormat.JSONEachRow)
    .dataCompression(ProtonCompression.brotli)
    .addDbParam(ProtonQueryParam.MAX_PARALLEL_REPLICAS, 2)
    .send();
```

3. Send data in binary formatted with custom user callback

```java
import ru.yandex.proton.ProtonStatement;
ProtonStatement sth = connection.createStatement();
sth.write().send("INSERT INTO test.writer", new ProtonStreamCallback() {
    @Override
    public void writeTo(ProtonRowBinaryStream stream) throws IOException {
        for (int i = 0; i < 10; i++) {
            stream.writeInt32(i);
            stream.writeString("Name " + i);
        }
    }
},
ProtonFormat.RowBinary); // RowBinary or Native are supported
```

</details>
