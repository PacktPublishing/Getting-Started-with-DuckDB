# Style Guide
In short - try to keep to the style as found in https://duckdb.org/docs


## SQL
- Keywords uppercase
- Column and table names lowercase

```sql
SELECT i, SUM(j) FROM tbl GROUP BY i;
```



# COPY
Formatting from https://duckdb.org/docs/sql/statements/copy.html

Options are in uppercase, values are in uppercase, booleans are in lowercase

```sql
COPY lineitem FROM 'lineitem.json' (FORMAT JSON, AUTO_DETECT true);

COPY lineitem TO 'lineitem.tsv' (DELIMITER '\t', HEADER false);
```


# READ_CSV
Formatting from https://duckdb.org/docs/data/csv/overview

Options and values in lowercase

```sql
SELECT * FROM read_csv('foo.csv', auto_detect=false, header=true, new_line='\n');
```

# DOT Commands

Use lowercase

```sql
.open newdb.db
```
