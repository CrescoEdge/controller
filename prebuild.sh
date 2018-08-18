#!/bin/bash

rm -rf src/main/resources/
mkdir src/main/resources/

mvn org.apache.maven.plugins:maven-dependency-plugin:get -DrepoUrl=https://oss.sonatype.org/content/repositories/snapshots -Dartifact=io.cresco:repo:1.0-SNAPSHOT
mvn org.apache.maven.plugins:maven-dependency-plugin:get -DrepoUrl=https://oss.sonatype.org/content/repositories/snapshots -Dartifact=io.cresco:sysinfo:1.0-SNAPSHOT
mvn org.apache.maven.plugins:maven-dependency-plugin:get -DrepoUrl=https://oss.sonatype.org/content/repositories/snapshots -Dartifact=io.cresco:dashboard:1.0-SNAPSHOT

cp ~/.m2/repository/io/cresco/repo/1.0-SNAPSHOT/repo-1.0-SNAPSHOT.jar src/main/resources/ 
cp ~/.m2/repository/io/cresco/sysinfo/1.0-SNAPSHOT/sysinfo-1.0-SNAPSHOT.jar src/main/resources/
cp ~/.m2/repository/io/cresco/dashboard/1.0-SNAPSHOT/dashboard-1.0-SNAPSHOT.jar src/main/resources/
