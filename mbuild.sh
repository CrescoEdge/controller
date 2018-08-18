rm src/main/resources/*
./prebuild.sh
mvn clean package bundle:bundle -U
cp target/controller-1.0-SNAPSHOT.jar ../agent/src/main/resources/
