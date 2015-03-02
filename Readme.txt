== Relational to Neo4j Importer

Scans Database Metadata to determine how to model
Uses Parallel Batch Importer to ingest data from all tables in parallel

=== Usage

Currently you need http://maven.apache.org/download.cgi[apache maven] installed.

Command line parameters are:

`import-rdbms-mvn.sh "jdbc:url" "schema" "graph.db"`

e.g.

`import-rdbms-mvn.sh "jdbc:mysql://localhost:3306/northwind?user=root" "northwind" "northwind.db"`


=== Transformation Rules

* Table with primary key become nodes
* FK (1:1, 1:n and n:1) become relationships
* Tables with no PK and exactly two FK and properties become relationships (optionally also if there is a pk)
* Tables with more than two FK become nodes
* remove all fk fields

=== Todo

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
