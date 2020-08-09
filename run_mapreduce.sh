# Script to run the Section 5.3 experiments
# (MapReduce)

cd examples/mapreduce
mvn test |& grep -v "WARNING:"
