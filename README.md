# DiffStream

A differential testing library for [Apache Flink](https://flink.apache.org/).

Note: This project is in an early stage of development. Stay tuned for updates!

## Installing the flink-training-exercises package

First run `git submodule init` and `git submodule update` in `flink-training-exercises` to obtain the source code.
Then build the code by running `mvn clean package`. Finally, install the package in the local Maven repository by
executing the following command in the top directory.

```sh
mvn install:install-file \
    -Dfile=flink-training-exercises/target/flink-training-exercises-2.9.1.jar \
    -DpomFile=flink-training-exercises/pom.xml \
    -Dsources=flink-training-exercises/target/flink-training-exercises-2.9.1-sources.jar \
    -Djavadoc=flink-training-exercises/target/flink-training-exercises-2.9.1-javadoc.jar
```

That finishes the installation. To see if everything is working, run `mvn test`.

## Installing Redis

First run `sudo apt-get install redis-server` and then use:

```sh
sudo systemctl start redis-server.service
```

to start the server.

## (Not needed) Installing Docker

First run `sudo apt install docker.io` and then use:

```sh
sudo systemctl start docker
```

to start the docker service and:

```sh
sudo usermod -a -G docker $USER
```

to give your user permission to execute docker containers.

## (Not needed) Installing OrientDB

Install docker and run:

```sh
docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=root orientdb:latest
```

## Starting the Java RMI registry

In order to run experiments on a standalone Flink cluster, we need
to use the sinks and matchers that communicate via Java RMI. These
are located in the package `edu.upenn.streamstesting.remote`. An
example of using the remote matcher can be found in
`edu.upenn.streamstesting.remote.RemoteTest`; the test itself still
runs on the `MiniClusterWithClientResource`, but it uses Java RMI.

Before using the remote matcher, it is necessary to start the
utility called `rmiregistry`. Start it like this:

```sh
rmiregistry -J-Djava.rmi.server.codebase=file:/home/filip/streams-testing/target/classes/ &
```

Change the path to wherever your target classes are located. Don't
forget the slash at the end.

