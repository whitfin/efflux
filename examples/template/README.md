# {{project_name}}

This project was generated using the Kickstart template.

You can run this project with or without Hadoop, using the following shell instructions:

```shell
# build the binaries
$ cargo build --release

# run with Hadoop Streaming
$ hadoop jar hadoop-streaming-2.8.2.jar \
    -input <INPUT> \
    -output <OUTPUT> \
    -mapper ./target/release/{{project_name}}_mapper \
    -reducer ./target/release/{{project_name}}_reducer

# run with Unix command shimming
$ cat <INPUT> | \
    ./target/release/{{project_name}}_mapper | \
    sort -k1,1 | \
    ./target/release/{{project_name}}_reducer \
    > <OUTPUT>
```
