echo "Usage: $0 jdbc:... schema graph.db"
JDBC=${1-jdbc:mysql://localhost:3306/northwind?user=root}
shift
SCHEMA=${1-northwind}
shift
DB=${1-northwind.db}
shift
if [ -d "$DB" ]; then rm -r "$DB"; fi
mvn compile exec:java -Dexec.mainClass="org.neo4j.imports.DatabaseImporter" -Dschemacrawler.log_level=SEVERE \
   -Dexec.args="$JDBC $SCHEMA $DB $*" 2>&1 | grep -v '\(INFO\|debug\|WARN\|SchemaCrawler\)'
