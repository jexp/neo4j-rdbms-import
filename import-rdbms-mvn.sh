#!/bin/bash -e
echo "Usage: $0 jdbc:... schema graph.db"
JDBC=${1-jdbc:mysql://localhost:3306/northwind?user=root}
shift
SCHEMA=${1-northwind}
shift
DB=${1-northwind.db}
shift
if [ -d "$DB" ]; then rm -r "$DB"; fi

mvn compile dependency:copy-dependencies

java -Xmx4G -cp $(for i in target/dependency/*.jar ; do echo -n $i":" ; done)target/neo4j-import-2.2-SNAPSHOT.jar org.neo4j.imports.DatabaseImporter $JDBC $SCHEMA $DB
