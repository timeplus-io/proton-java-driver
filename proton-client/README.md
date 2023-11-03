# Proton Java Client

Async Java client for Proton. `proton-client` is an abstract module, so it does not work by itself until being used together with an implementation like `proton-grpc-client` or `proton-http-client`.


## Quick Start

```xml
<dependency>
    <groupId>com.timeplus</groupId>
    <artifactId>proton-http-client</artifactId>
    <version>0.3.2-patch7</version>
</dependency>
```

```java
// declare a server to connect to
ProtonNode server = ProtonNode.of("server1.domain", ProtonProtocol.HTTP, 8123, "my_db");

// execute multiple queries in a worker thread one after another within same session
CompletableFuture<List<ProtonResponseSummary>> future = ProtonClient.send(server,
    "create database if not exists test",
    "use test", // change current database from my_db to test
    "create table if not exists test_table(s String) engine=Memory",
    "insert into test_table values('1')('2')('3')",
    "select * from test_table limit 1",
    "truncate table test_table",
    "drop table if exists test_table");

// block current thread until queries completed, and then retrieve summaries
// List<ProtonResponseSummary> results = future.get();

try (ProtonClient client = ProtonClient.newInstance(server.getProtocol())) {
    ProtonRequest<?> request = client.connect(server).format(ProtonFormat.RowBinaryWithNamesAndTypes);
    // load data into a table and wait until it's completed
    request.write().query("insert into my_table select c2, c3 from input('c1 UInt8, c2 String, c3 Int32')")
        .data(myInputStream).execute().thenAccept(response -> {
	        response.close();
        });

    // query with named parameter
    try (ProtonResponse response = request.query(
            ProtonParameterizedQuery.of(
                request.getConfig(),
                "select * from numbers(:limit)")
            ).params(100000).execute().get()) {
        for (ProtonRecord r : response.records()) {
            // Don't cache ProtonValue / ProtonRecord as they're reused for
            // corresponding column / row
            ProtonValue v = r.getValue(0);
            // converts to DateTime64(6)
            LocalDateTime dateTime = v.asDateTime(6);
            // converts to long/int/byte if you want to
            long l = v.asLong();
            int i = v.asInteger();
            byte b = v.asByte();
        }
    }
}
```
