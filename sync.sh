#!/bin/bash
lein compile && lein javac && lein uberjar && storm jar target/dataserver.jar echo.dataserver.repl sync conf/local.config
