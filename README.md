# streams-testing

## Installing the flink-training-exercises package

First run `git submodule init` and `git submodule update` in `flink-training-exercises` to obtain the source code.
Then build the code by running `mvn clean package`. Finally, install the package in the local Maven repository by
executing the following command in the top directory.

```sh
mvn install:install-file \
    -Dfile=flink-training-exercises/target/flink-training-exercises-2.9.0.jar \
    -DpomFile=flink-training-exercises/pom.xml \
    -Dsources=flink-training-exercises/target/flink-training-exercises-2.9.0-sources.jar \
    -Djavadoc=flink-training-exercises/target/flink-training-exercises-2.9.0-javadoc.jar
```

## Generating input

KK: How should we generate the data items of input streams? Maybe a
good start would be to use the input generators from some property
based testing java library? Does anyone have a better idea?

## Possible example sources

I gathered some Flink examples that I could find with a search on
Github. For now I just put the links here, but at some point we can
look deeper in them if needed.

- (100 stars) Example of monitoring data center temperatures: https://github.com/tillrohrmann/cep-monitoring It is about monitoring the temperatures of different racks (in a datacenter) and producing warnings and alerts for each rack. It seems to me that the alerts and warnings for each rack are fully dependent per rack. What kind of bug could we uncover in this situation?

- (80 stars) Examples from O'Reilly's "Stream processing with Apache Flink": https://github.com/streaming-with-flink/examples-scala

- (165 stars) Examples in Python's Flink API (We might want to transcribe them, if they seem interesting): https://github.com/wdm0006/flink-python-examples

- (129 stars) A big data application. It includes a part in Flink (I don't know if that is interesting on its own) but it interfaces with Kafka and other so this might be a bit annoying to integrate for our evaluation. https://github.com/Chabane/bigdata-playgroun

- (452 stars) Yahoo streaming benchmarks. This seems like a pretty complex computation. https://github.com/yahoo/streaming-benchmarks. This is very interesting. On the first sight, it seems that everything is a map. But the map functions are rich flat maps, which have access to some other datastore, where they __both__ write and read. I am not sure if these read and write are in independent locations, but we should certainly try to include this in our evaluation. However, we will need to find a way to interface with it, because it doesn't have a sink, but rather events are written to some other data store at the end of the datastream.

## TODO

- The generator should also create timestamps (if an application talks
  about event time). In that case, we should also be able to configure
  the frequency of generation, because systems might exhibit bad
  behaviours when pushed to their limits.

