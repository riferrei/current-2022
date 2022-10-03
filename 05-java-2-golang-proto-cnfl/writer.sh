#!/bin/bash

mvn clean package

java -jar target/05-java-2-golang-proto-cnfl-1.0.jar
