=================
duke-microservice
=================

A Java microservice that uses the Duke deduplication engine (https://github.com/larsga/Duke).


To run it:

    docker run -p 4567:4567 knutj42/sesam-duke-microservice

Then open a browser on http://localhost:4567


To build a new version of the duke microservice:

    mvn clean install

    docker build -t knutj42/sesam-duke-microservice .
