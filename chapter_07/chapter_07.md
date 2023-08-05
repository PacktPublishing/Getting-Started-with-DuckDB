# Chapter 7: Using the DuckDB Python API

## The Relational API

```python
result = duckdb.sql("SELECT 'duck' AS animal, 'quack!' as greeting")
type(result)
```

### Dataset for exploration

### Data ingestion

```python
pets_csv = duckdb.read_csv("Seattle_Pet_Licenses.csv")
pets_csv.show()
```
```python
pets_csv.types
```

```python
help(duckdb.read_csv)
```

```python
pets_csv = duckdb.read_csv(
    "Seattle_Pet_Licenses.csv",
    dtype={"License Issue Date": duckdb.typing.DATE},
    date_format="%B %d %Y",
)

# TODO: this is a workaround for this bug
# https://github.com/duckdb/duckdb/issues/8209
pets_csv.query("", "FROM pets_csv").types
```

```python
pets_csv.limit(5)
```

```python
pets_csv.sql_query()
```

```python 
pets_csv_alt = duckdb.sql(
    """
    SELECT *
    FROM read_csv_auto(
        'Seattle_Pet_Licenses.csv', 
        dateformat='%B %d %Y', 
        dtypes={'License Issue Date': 'DATE'}
    )
    """
)
```

### Querying relations

```python
pets = duckdb.sql(
    """
    SELECT 
        "License Issue Date" AS issue_date,
        "Animal's Name" AS pet_name,
        "Species" as species,
        "Primary Breed" AS breed
    FROM pets_csv
    """
)

pets.limit(5)
```

```python
duckdb.sql("SELECT min(issue_date), max(issue_date) FROM pets")
```

### Working with relations 

```python
pets.value_counts("species")
```

```python
pets.filter("species = 'Pig'")
```

```python
pets.filter("species = 'Dog'").value_counts("pet_name").order("2 DESC").limit(10)
```

```python
val_counts_sql = pets.filter("species = 'Dog'").value_counts("pet_name").order("2 DESC").limit(10).sql_query()

print(val_counts_sql)
```

```python
import sqlparse

val_counts_sql_cleaned = sqlparse.format(val_counts_sql, reindent=True)

print(val_counts_sql_cleaned)
```

```python
pets.order("length(pet_name) DESC").limit(10)
```

```python editable=true slideshow={"slide_type": ""}
pets.query("pets_rel", "SELECT *, length(pet_name) AS name_length FROM pets_rel").order(
    "name_length DESC"
).limit(10)
```

### Writing to disk

```python
pets.write_csv("seattle_pets.csv", header=True)
pets.write_parquet("seattle_pets.parquet")
```

```python
duckdb.sql("COPY pets TO 'seattle_pets.csv' (HEADER TRUE)")
duckdb.sql("COPY pets TO 'seattle_pets.parquet'")
```

### Modifying the database

```python
conn = duckdb.connect("seattle_pets.db")
```

```python
conn.read_parquet("seattle_pets.parquet").create("pets")
```

```python
conn.sql("SHOW TABLES")
```

```python
conn.table("pets").count("*")
```

```python
new_dog1 = ("2023-07-16", "Monty", "Dog", "Border Collie")
conn.table("pets").insert(new_dog1)
```

```python
new_dog2 = ("2023-07-16", "Pixie", "Dog", "Australian Kelpie")
new_dog_rel = conn.values(new_dog2)
new_dog_rel.insert_into("pets")
```

```python
conn.table("pets").filter("issue_date = '2023-07-16'")
```

```python
conn.close()
```


## The Python DB-API

### Connecting to a database

```python
conn = duckdb.connect()
```

### Querying databases

```python
conn.execute("CREATE TABLE seattle_pets AS SELECT * FROM 'seattle_pets.parquet'")
```

```python
conn.execute("SELECT * FROM seattle_pets")
```

```python
conn.fetchone()
```

```python
conn.description
```

```python
[conn.fetchone() for i in range(3)]
```

```python
conn.fetchmany(3)
```

```python
rest_rows = conn.fetchall()
len(rest_rows)
```

```python
conn.execute?
```

### Prepared statements

```python
import datetime

new_pet1 = (datetime.date.today(), "Ned", "Dog", "Border Collie")
conn.execute("INSERT INTO seattle_pets VALUES (?, ?, ?, ?)", parameters=new_pet1)
```

```python
new_pet2 = {
    "name": "Simon",
    "species": "Cat",
    "breed": "Bombay",
    "issue_date": datetime.date.today(),
}
conn.execute(
    "INSERT INTO seattle_pets VALUES ($issue_date, $name, $species, $breed)", new_pet2
)
```

```python
conn.execute(
    """
    SELECT *
    FROM seattle_pets
    WHERE issue_date = ?;
    """,
    [datetime.date.today()],
).fetchall()
```

### Writing to disk

```python
conn.execute("COPY seattle_pets TO 'seattle_pets_updates.csv' (HEADER TRUE)")

conn.execute("COPY seattle_pets TO 'seattle_pets_updates.parquet'")
```

### Closing the database connection

```python
conn.close()
```

### Cursors


## Python Integration

### Consuming other data structures

#### Querying Python objects via replacement scans

```python
import pandas as pd

pets_df = pd.read_parquet("seattle_pets.parquet").sample(frac=1)
duckdb.sql("SELECT * FROM pets_df").fetchone()
```

#### Registering objects as virtual tables

```python
pets_dict = {"seattle": pd.read_parquet("seattle_pets.parquet").sample(frac=1)}

duckdb.register("pets_from_pandas", pets_dict["seattle"])
duckdb.sql("SELECT * FROM pets_from_pandas").fetchone()
```

#### Creating tables from objects

```python
pets_df = pd.read_parquet("seattle_pets.parquet").sample(frac=1)
duckdb.sql("CREATE OR REPLACE TABLE pets_table_from_df AS SELECT * FROM pets_df")
duckdb.sql("SELECT * FROM pets_table_from_df").fetchone()
```

### Result conversion

#### Converting to DataFrames

```python
conn = duckdb.connect()
seattle_pets = conn.from_parquet("seattle_pets.parquet")
pets_df = seattle_pets.df()
pets_df[pets_df["species"] == "Dog"].value_counts("breed")[:5]
```

```python
import polars as pl

pets_df = seattle_pets.pl()
pets_df.filter(pl.col("species") == "Cat")["breed"].value_counts(sort=True)[:5]
```

#### Converting to Arrow Tables

```python
conn = duckdb.connect()
conn.execute("SELECT * FROM 'seattle_pets.parquet'")
pets_table = conn.arrow()
pets_table.schema
```

### Typing: from Python to DuckDB

```python
varchar_type = duckdb.typing.VARCHAR
bigint_type = duckdb.typing.BIGINT
```

```python
varchar_type = duckdb.typing.DuckDBPyType(str)
bigint_type = duckdb.typing.DuckDBPyType(int)
```

```python
duckdb.values(
    [
        10,
        1_000_000,
        0.95,
        "hello string",
        b"hello bytes",
        True,
        datetime.date.today(),
        None,
    ]
)
```

```python
duckdb.values([(1, 2), ["hello", "world"], {"key1": 10, "key2": "quack!"}])
```

### User-defined functions

```python
import emoji

def emojify(species):
    """Converts a string into a single emoji, if possible."""
    emoji_str = emoji.emojize(f":{species.lower()}:")
    if emoji.is_emoji(emoji_str):
        return emoji_str
    return None

emojify("goat")
```
```python
duckdb.create_function(
    "emojify", emojify, [duckdb.typing.VARCHAR], duckdb.typing.VARCHAR
)
```

```python
duckdb.sql(
    """
    SELECT *, 
        emojify(species) as emoji
    FROM 'seattle_pets_updates.parquet'
    USING SAMPLE 10
    """ 
)
```

```python
duckdb.remove_function("emojify")
```

```python
duckdb.create_function("emojify", emojify, [str], str)
```

```python
def emojify(species: str) -> str:
    """Converts a string into a single emoji, if possible."""
    emoji_str = emoji.emojize(f":{species.lower()}:")
    if emoji.is_emoji(emoji_str):
        return emoji_str
    return None
```

```python
duckdb.create_function("emojify", emojify)
duckdb.remove_function("emojify")
```

### Handling exceptions

```python
from duckdb import ConversionException

try:
    duckdb.execute("SELECT '5,000'::INTEGER").fetchall()
except ConversionException as error:
    print(error)
    # handle exception...
```

```python
from duckdb import CatalogException

try:
    duckdb.sql("SELECT * from imaginary_table")
except CatalogException as error:
    print(error)
    # handle exception...
```

# Conclusion
