# yarn-flink-examples
A few examples with flink

NOTICE: To execute them, some change may be required.

## How to build
In the corresponding directory, execute <code>mvn package</code>

## How to execute
In the corresponding directory, execute <code>flink run -m yarn-cluster -yn 1 -yjm 1024 -ytm 1024 target/flink-*.jar</code>
