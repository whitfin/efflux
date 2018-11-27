# Word Counter

This example is based on the traditional MapReduce word counter example. Given an input file, this job will count the number of times each word appears. Sample input and outputs are provided in the `data` directory.

You can run this example with or without Hadoop, using the following shell instructions:

```shell
# build the binaries
$ cargo build --release

# run with Hadoop Streaming
$ hadoop jar hadoop-streaming-2.8.2.jar \
    -input ./data/input.txt \
    -output ./output \
    -mapper ./target/release/wordcount_mapper \
    -reducer ./target/release/wordcount_reducer

# run with Unix command shimming
$ cat ./data/input.txt | \
    ./target/release/wordcount_mapper | \
    sort -k1,1 | \
    ./target/release/wordcount_reducer \
    > ./output/output.txt
```
