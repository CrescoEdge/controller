cd ..
cd dashboard
./build.sh
cd ..
cd sysinfo
./build.sh
cd ..
cd repo
./build.sh
cd ..
cd controller
mvn clean package bundle:bundle -U
cp target/controller-1.0-SNAPSHOT.jar ../agent/src/main/resources/
