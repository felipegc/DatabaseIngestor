install ojdbc6 locally so maven can see it by running the following command under bin dir.

mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.4 -Dpackaging=jar -Dfile=ojdbc6.jar -DgeneratePom=true



#Run
#Assuming you are under the root
mvn clean
mvn compile
mvn package
java -cp  java -cp <path-to-schema-conversion-jar>:<path-to-ojdbc6>:<path-to-database-injestor-jar> com.magicbq.ingestor.App <path-to-properties-file>

i.e: java -cp ./bin/schema_conversion_library.jar:bin/ojdbc6.jar:target/DatabaseIngestor-1.0-SNAPSHOT.jar com.magicbq.ingestor.App setup.properties# DatabaseIngestor
