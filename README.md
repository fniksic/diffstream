# streams-testing


## Generating input

KK: How should we generate the data items of input streams? Maybe a
good start would be to use the input generators from some property
based testing java library? Does anyone have a better idea?

## Possible example sources

I gathered some Flink examples that I could find with a search on
Github. For now I just put the links here, but at some point we can
look deeper in them if needed.

- (100 stars) Example of monitoring data center temperatures: https://github.com/tillrohrmann/cep-monitoring

- (80 stars) Examples from O'Reilly's "Stream processing with Apache Flink": https://github.com/streaming-with-flink/examples-scala

- (165 stars) Examples in Python's Flink API (We might want to transcribe them, if they seem interesting): https://github.com/wdm0006/flink-python-examples

- (129 stars) A big data application. It includes a part in Flink (I don't know if that is interesting on its own) but it interfaces with Kafka and other so this might be a bit annoying to integrate for our evaluation. https://github.com/Chabane/bigdata-playgroun

- (452 stars) Yahoo streaming benchmarks. This seems like a pretty complex computation. https://github.com/yahoo/streaming-benchmarks

## TODO

- The generator should also create timestamps (if an application talks
  about event time). In that case, we should also be able to configure
  the frequency of generation, because systems might exhibit bad
  behaviours when pushed to their limits.

