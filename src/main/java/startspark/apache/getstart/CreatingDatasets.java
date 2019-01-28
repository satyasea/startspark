package startspark.apache.getstart;
// http://spark.apache.org/docs/2.4.0/sql-getting-started.html#creating-dataframes

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

public class CreatingDatasets {

    private static String dir = "C:\\Users\\wolf\\IdeaProjects\\startspark\\";
    private static String inputFile = "people.json";
    private static String inputURL = dir+inputFile;

    public static void main (String []args ){

        SparkSession spark = SparkSession
                .builder()
                .appName("Create Data Frame Dataset")
                .config("spark.master", "local")
                .getOrCreate();

        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);
        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();


        // Encoders for most common types are provided in class Encoders

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        primitiveDS.show();
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]
        transformedDS.show();


        Dataset<Person> peopleDS = spark.read().json(inputURL).as(personEncoder);
        peopleDS.show();

        spark.close();
    }

    public static class Person implements Serializable {

        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

    }
}
