#!/bin/bash

mvn clean package -DskipTests=true

java -jar target/07-cnfl-sr-2-aws-glue-migration-1.0.jar w
