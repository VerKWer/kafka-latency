# kafka-latency
Simple latency test for Apache Kafka

## Dependencies
You need to have `gnuplot` installed and accessible on your system. And obviously, you need to have Kafka up and
running.

## Installation
1. `git clone https://github.com/VerKWer/kafka-latency.git`
2. `cd kafka-latency`
3. `gradle build`

## Run
From the `kafka-latency` directory, where you built the project, simply call `java -jar build/libs/kafka-latency.jar`

## Runtime Parameters
By default, the local hostname is used as the bootstrap server for kafka (with port 9092). If your setup is different
(e.g. if you're running the latency test on a machine that is not part of the Kafka cluster), then simply call  
`export KAFKA_BOOTSTRAP_SERVERS=...`  
before running the jar-file. The value of this variable is a comma-separated list of bootstrap servers plus port for
Kafka. So, for example,  
`export KAFKA_BOOTSTRAP_SERVERS=localhost:9092`  
would be valid.

Also by default, the latency test runs for approximately 10 seconds at 200 messages per second, each of size 100B and
these are posted to a topic called "`LatencyTestTopic`" with a replication factor of 1. Moreover, we always try to force
the topic leader to not be broker number 1 (we assume that the benchmark runs on the same machine that is also broker
number 1). All these things can be manually specified as command line parameters. Say we would like to run a latency
test for 7 seconds at 500 messages per second of size 200B each that are posted to the topic "`Test`" with a replication
factor of 3 and where we try to force broker number 2 to be the leader. For this, we would call  
`java -jar build/libs/kafka-latency.jar 7s 500/s 200B topic=Test rf=3 lead=2`.

## Output
By default, the output plot is not stored anywhere. If you would like to store a copy of the plot on disk, just pass the
output file name (which has to end with `.png`) as a command line parameter. So, for instance, calling  
`java -jar build/libs/kafka-latency.jar out.png`  
would store the resulting plot in a file called `out.png` in the current directory.
