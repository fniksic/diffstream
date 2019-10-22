# streams-testing

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

## Generating input

KK: How should we generate the data items of input streams? Maybe a
good start would be to use the input generators from some property
based testing java library? Does anyone have a better idea?

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

## Possible example sources

I gathered some Flink examples that I could find with a search on
Github. For now I just put the links here, but at some point we can
look deeper in them if needed.

- (100 stars) Example of monitoring data center temperatures: https://github.com/tillrohrmann/cep-monitoring It is about monitoring the temperatures of different racks (in a datacenter) and producing warnings and alerts for each rack. It seems to me that the alerts and warnings for each rack are fully dependent per rack. What kind of bug could we uncover in this situation?

- (80 stars) Examples from O'Reilly's "Stream processing with Apache Flink": https://github.com/streaming-with-flink/examples-scala

- (165 stars) Examples in Python's Flink API (We might want to transcribe them, if they seem interesting): https://github.com/wdm0006/flink-python-examples

- (129 stars) A big data application. It includes a part in Flink (I don't know if that is interesting on its own) but it interfaces with Kafka and other so this might be a bit annoying to integrate for our evaluation. https://github.com/Chabane/bigdata-playgroun

- (452 stars) Yahoo streaming benchmarks. This seems like a pretty complex computation. https://github.com/yahoo/streaming-benchmarks. This is very interesting. On the first sight, it seems that everything is a map. But the map functions are rich flat maps, which have access to some other datastore, where they __both__ write and read. I am not sure if these read and write are in independent locations, but we should certainly try to include this in our evaluation. However, we will need to find a way to interface with it, because it doesn't have a sink, but rather events are written to some other data store at the end of the datastream.

- Flink-examples with side output: https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/sideoutput/SideOutputExample.java. Maybe the way to have a non-trivial output dependency relation would be by also considering side outputs in the testing process. However, usually it seems that Flink developers use side-outputs for discarding invalid data items. Can we find a usage of side-outputs where they output real data items?

- All the flink-streaming examples: https://github.com/apache/flink/tree/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples. It would be amazing if we manage to find a bug in them (though I doubt it).
  + The iterative examples seems to be just maps, so I don't think
    that we can uncover some error there.
  + The windowed join example seems to be a non-trivial computation
    and I am wondering whether increasing parallelism could alter the
    results
  + The incremental learning skeleton is a skeleton for incremental
    machine learning tasks, where one has a partial model built from
    historical data, and updates it everytime new data arrive. There
    is no real computation happening in this one however. It is just a
    skeleton.

## TODO

- We could have a testing mode where our framework (given a program),
  creates two configurations, one with aprallelism 1 everywhere, and
  one where parallelism is the highest (or ranges) up to some user
  given bound for all (or some nodes). This would allow a user that is
  want to deploy a flink program to feel more confident with using
  parallelism.

- Documentation for Flink configuration: 
  https://ci.apache.org/projects/flink/flink-docs-stable/dev/execution_configuration.html

- The generator should also create timestamps (if an application talks
  about event time). In that case, we should also be able to configure
  the frequency of generation, because systems might exhibit bad
  behaviours when pushed to their limits.

