#!/bin/bash -e
echo "Usage: $0 jdbc:... schema graph.db"
JDBC=${1-jdbc:mysql://localhost:3306/northwind?user=root}
shift
SCHEMA=${1-northwind}
shift
DB=${1-northwind.db}
shift
if [ -d "$DB" ]; then rm -r "$DB"; fi

mvn clean package dependency:copy-dependencies

CP=target/neo4j-import-2.2.0.jar

for i in target/dependency/*.jar ; do CP="$CP:$i"; done
echo $CP

echo java -Xmx4G -cp $CP org.neo4j.imports.DatabaseImporter $JDBC $SCHEMA $DB
java -Xmx4G -cp $CP org.neo4j.imports.DatabaseImporter $JDBC $SCHEMA $DB
