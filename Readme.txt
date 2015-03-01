Relational to Neo4j Importer

Scans Database Metadata to determine how to model
Uses Parallel Batch Importer to ingest data from all tables in parallel

Transformation Rules

* Table with primary key become nodes
* FK (1:1, 1:n and n:1) become relationships
* Tables with no FK exactly two FK and properties become relationships (optional also if there is a pk)
* Tables with two or more FK become nodes
* remove all fk fields

Todo

* Table name, column name transformation
* Modeling Rules by table name pattern
* Type transformation
* Skipping Columns / Rows
* Unique Constraints
* Indexes
* Compound Indexes (create artificial property)
* Skip keys
* Limit row count, by time
* Custom SQL statements
* Configure concurrency for JDBC reads
