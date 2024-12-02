#!/bin/sh

javac -cp "classes:lib/*" -d . src/*.java

jar cfmv RecordSimulatorClient.jar manifest.txt snowflake.properties rsa_key.p8 DATA.csv *.class
