package spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;


public class BiGramsApp {

    public static void main(String[] args) throws IOException {
        String input = args[0];
        String outputDir = args[1];

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("BiGrams");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        // Load input
        JavaRDD<String> sentences = sparkContext.textFile(input);
        //for( String x : sentences)
        List<String> tweets = sentences.collect();
        for(String tweet: tweets) {
            Optional<ExtendedSimplifiedTweet> prelimTweet = ExtendedSimplifiedTweet.fromJson(tweet);
            if (prelimTweet.isPresent()) {
                String text = prelimTweet.get().getText();
                System.out.println(text + " nextext ");
            }
        }


        /*JavaPairRDD<String, Integer> counts = sentences
                .flatMap(s -> Arrays.asList(s.split("[ ]")).iterator())
                .map(BiGramsApp::normalise);

         */




        /*
        .mapToPair(word -> new Tuple2<>(word, 1));
        .reduceByKey(Integer::sum);
        System.out.println("Total words: " + counts.count());
        counts.saveAsTextFile(outputDir);

         */
    }

    private static String normalise(String word) {
        return word.trim().toLowerCase();
    }
}
