Learning Apache Spark

Starting first with example/JavaWordCount, then example/streaming/JavaNetworkWordCount (unfinished), and example/study/RDDWordCount as a prototype for other Apache Spark Examples

Modified the Spark code tutorial examples to run within an IDE, fixed it for clarity, made comments, and cobbled code together to make it compile or run, to break apart the code and use the API.

To show a simple way of looking at it, example/study/MapReduce is a Java program with a list put in a map.

https://spark.apache.org/examples.html
Intro From Apache web site:
These examples give a quick overview of the Spark API. Spark is built on the concept of distributed datasets, which contain arbitrary Java or Python objects. You create a dataset from external data, then apply parallel operations to it. The building block of the Spark API is its RDD API. In the RDD API, there are two types of operations: transformations, which define a new dataset based on previous ones, and actions, which kick off a job to execute on a cluster. On top of Spark’s RDD API, high level APIs are provided, e.g. DataFrame API and Machine Learning API. These high level APIs provide a concise way to conduct certain data operations. In this page, we will show examples using RDD API as well as examples using high level APIs.
