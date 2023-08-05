# Chapter 6: Installing and connecting to DuckDB in Python

## Connecting to DuckDB in Python

### The default in-memory database

```python
import duckdb

duckdb.sql("SELECT 'duck' AS animal, 'quack!' AS greeting")
```

```python
duckdb.install_extension("httpfs")
duckdb.load_extension("httpfs")
```

### Managing database connections explicitly

```python
conn = duckdb.connect(database=":memory:")
conn.sql("CREATE OR REPLACE TABLE hello AS SELECT 'duck' AS animal, 'quack!' AS greeting")
conn.sql("SELECT * FROM hello")
```

### Connections to persistent-storage databases

```python
conn = duckdb.connect(database="quack.duckdb")
conn.sql("CREATE OR REPLACE TABLE hello AS SELECT 'duck' AS animal, 'quack!' AS greeting")
conn.sql("SELECT * FROM hello").show()
conn.close()
```

```python
# TODO: double check this works on windows
! duckdb quack.duckdb -c 'SELECT * FROM hello'
```

### Closing database connections

```python
with duckdb.connect(database="quack.duckdb") as conn:
    conn.sql("INSERT INTO hello VALUES ('Labradorius', 'quack!')")
    conn.sql("SELECT * FROM hello").show()
```

### Sharing disk-based databases between processes

```python
conn = duckdb.connect(database="quack.duckdb", read_only=True)
# query the database in here...
conn.close()
```
