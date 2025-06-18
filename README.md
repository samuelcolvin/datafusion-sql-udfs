# SQL UDF Demo

Requires https://github.com/apache/datafusion/pull/14837.

Performance with 10M rows:

```
[src/main.rs:169:5] df.count().await.unwrap() = 10000000
Elapsed time (direct): 133.333µs
[src/main.rs:184:5] df.count().await.unwrap() = 10000000
Elapsed time (SQL UDF): 132.959µs
```
