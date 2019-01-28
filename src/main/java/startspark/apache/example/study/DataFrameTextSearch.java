
package startspark.apache.example.study;

/*
This class is a prototype for building generic study examples from Spark web site
https://spark.apache.org/examples.html
The code is cobbled from a few other Spark examples and provides a spark context converted to a javasparkcontext
Some renaming for clarity
breaking down the single line code, unchaining anonymous objects into steps
making objects type visible and using the objects directly.
 */
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import static org.apache.spark.sql.functions.col;

public class DataFrameTextSearch {

    private static String dir = "C:\\Users\\wolf\\IdeaProjects\\startspark\\";
    private static String inputFile = "text-search.txt";
    private static String inputURL = dir+inputFile;


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
        JavaRDD<String> textFile = jsc.textFile(inputURL, 0);
// Creates a DataFrame having a single column named "line"
        JavaRDD<Row> rowRDD = textFile.map(RowFactory::create);

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("line", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);


        SQLContext sqlContext = new SQLContext(jsc);
        //DataFrame df = sqlContext.createDataFrame(rowRDD, schema);
        //use Dataset
       Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema);
        System.out.println("showing all");
        df.show();

        System.out.println("showing dataset filtered on errors");
        Dataset<Row> errors = df.filter(col("line").like("%error%"));
        errors.show();
        System.out.println("showing error count");
        long errorCount = errors.count();
        System.out.println("error count = " + errorCount);


// Fetches the foo errors as an array of strings
        errors.filter(col("line").like("%foo%")).collect();
            System.out.println("showing all foo errors");
        errors.show();
        // Counts errors mentioning foo
        System.out.println("showing count of foo");
        long errorFooCount = errors.filter(col("line").like("%foo%")).count();
        System.out.println("error count = " + errorFooCount);

        //close session
        session.stop();
    }
}
