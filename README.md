# D2IA: Data-driven Interval Analytics
D2IA is a Flink library that uses Flink CEP to declaratively define event intervals and reason about their relationships using Allen's interval algebra.

We provide three operators:
* Homogeneous (HoIE) interval generator: this takes as input a single source stream, a condition, a number of occurrences or a time window,
* Heterogeneous (HeIE) interval generator: this takes as input at least two source streams. Similarly, it takes a condition, or a time window,
* Interval operator: this always finding matches between pairs of interval events or an interval event and an instantaneous 

A condition can be either absolute, i.e., stateless or relative, i.e., stateful. An absolute condition, refers to properties of candidate events solely to determine their membership to the interval. A relative condition, on the other hand, compares the candidate event to expressions evaluated over the events added to the interval so far. 

## Example usage

You can run the test class on path ee.ut.cs.dsg.example.linearroad.LinearRoadRunner with the following parameters

--source [kafka|file] --jobType [ThresholdAbsolute|ThresholdRelative|Delta|Aggregate] --fileName [path to file] --kafka [comma separated list of [ip:port] for the bootstrap servers] --topic [kafka topic to read linear road data from]

This runs a predefined group of interval specifications against the linar road data set.

A data set containing about 24M records of linear road data can be found [here](https://tartuulikool-my.sharepoint.com/:x:/g/personal/ahmed79_ut_ee/EZqIjWd95FtJjL4vJzQkR4MBJzKs-uFAYUaWJJQCy0t72g?e=5rdSbM)

Parameters description
1. Source: Identifies the source type, either a path or url to a csv file or, Kafka to point to the source of the data,
2. JobType: you can choose from: ThresholdAbsolute, ThresholdRelative, Delta or Aggregate. These are predefined intervals created by the HoIE. To understand the concepts behind these different types of data driven intervals, you may want to check [this](https://kops.uni-konstanz.de/bitstream/handle/123456789/33848/DEBS2016.pdf?sequence=1) paper,
3. fileName: is the path to a csv file on the form VID,SPEED,ACCEL,XWay,Lane,Dir,Seg,Pos,T1,T2. An example file was ponited to above.
4. Kafka: this is a comma separated list of ip:port for the bootstrap servers for Kafka,
5. Topic: is the topic name from which the data on the same format as in point 3 will be pulled.
