package startspark.apache.example.study;

import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class RDDWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static String dir = "C:\\Users\\wolf\\IdeaProjects\\startspark\\";
    private static String inputFile = "README.md";
    private static String inputURL = dir+inputFile;
    private static String outputFile = "RDDWordCountOutput";
    private static int numPartitionsInOutput = 2;

    public static void main (String args[]){
        //initialize build config session
        SparkSession session = SparkSession
                .builder()
                .appName("RDDWordCount")
                //required to run on local machine
                .config("spark.master", "local")
                .getOrCreate();
        //run
        runWordCount(session);
    }

    //encapsulate main function
    private static void runWordCount(SparkSession session){
        //obtain javasparkcontext from session in which the file is read into an RDD
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(session.sparkContext());
        JavaRDD<String> textFile = jsc.textFile(inputURL, numPartitionsInOutput);
      // JavaPairRDD<String, Integer> wordToCountMap = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);

        // JavaRDD<String> lines = session.read().textFile(inputURL).javaRDD();
        JavaRDD<String> lines = textFile.rdd().toJavaRDD();
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordToOneMap = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordToCountMap = wordToOneMap.reduceByKey((a, b) -> a + b);

        //display output
        List<Tuple2<String, Integer>> output = wordToCountMap.collect();
        System.out.println("Display Count output");
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        System.out.println("Done displaying output");

        //store results
        wordToCountMap.saveAsTextFile(dir + outputFile);

        //close session
        session.stop();
    }
}
