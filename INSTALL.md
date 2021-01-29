# Installation Instructions

## Installing Java 11 and Maven

You will need Java version 11.

- On Linux systems, check if java is installed with `java -version` and `javac -version`. Both commands should output version 11. If not, run `sudo apt install openjdk-11-jdk`, `sudo apt install default-jdk`, and finally `sudo update-alternatives --config java` to select version 11 as the default.

  Next, make sure the `JAVA_HOME` environment variable points to the correct version of Java. To check if it is set, run `echo $JAVA_HOME` (look for `java-11` in the output). Otherwise, set it with `export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64/"` (or the path to wherever Java 11 is installed. Add this line to `~/.bashrc` so that it runs automatically whenever you open a terminal.

  Finally, Maven needs to be installed and point to Java 11: `mvn --version` should show Java version 11. If it doesn't work, install with `sudo apt install maven`, and also ensure that `JAVA_HOME` is set correctly.

## Installing the flink-training-exercises package

Inside the `flink-training-exercises` subdirectory (`cd flink-training-exercises`), run `git submodule init` and `git submodule update`. Then build the code by running `mvn clean package`.

Back in the top-level directory (`cd ..`), install the package in the local Maven repository by executing the following command:

```sh
mvn install:install-file \
    -Dfile=flink-training-exercises/target/flink-training-exercises-3.1.1.jar \
    -DpomFile=flink-training-exercises/pom.xml \
    -Dsources=flink-training-exercises/target/flink-training-exercises-3.1.1-sources.jar
```

That finishes the installation. To see if everything is working, run `mvn test`.

## Installing DiffStream

Make sure that you have installed `flink-training-exercises` as described above.

Then run `mvn clean package` to build DiffStream.

Then install it by running:

```sh
cd parent
mvn install
cd ..
mvn install
```

## Installing Redis

First run `sudo apt-get install redis-server` and then use:

```sh
sudo systemctl start redis-server.service
```

to start the server.

<!-- ## (Not needed) Installing Docker

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
``` -->

## Starting the Java RMI registry

In order to run experiments on a standalone Flink cluster, we need
to use the sinks and matchers that communicate via Java RMI. These
are located in the package `edu.upenn.diffstream.remote`. An
example of using the remote matcher can be found in
`edu.upenn.diffstream.remote.RemoteTest`; the test itself still
runs on the `MiniClusterWithClientResource`, but it uses Java RMI.

Before using the remote matcher, it is necessary to start the
utility called `rmiregistry`. Start it like this:

```sh
rmiregistry -J-Djava.rmi.server.codebase=file:/home/filip/streams-testing/target/classes/ &
```

Change the path to wherever your target classes are located. Don't
forget the slash at the end.
