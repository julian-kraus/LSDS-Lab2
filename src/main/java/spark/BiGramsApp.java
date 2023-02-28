package spark;

import edu.upf.model.SimplifiedTweet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;


public class BiGramsApp {
    private static final Pattern SPACE = Pattern.compile(" ");


    public static void main(String[] args) throws IOException {
        String input = args[0];
        String outputDir = args[1];
        String language = args[2];

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("BiGrams");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load input
        JavaRDD<String> line = sparkContext.textFile(input); // creates an RDD of strings, where each string represents a line in the file

        // Filter each line by language we want for one in specific
        JavaRDD<String> langline = line.filter(s -> {
            Optional<ExtendedSimplifiedTweet> tweet = ExtendedSimplifiedTweet.fromJson(s);
            return tweet.isPresent() && tweet.get().getLanguage().equals(language) && !tweet.get().isRetweeted();
        });

        // Split filtered by language lines into words that will be normalized
        JavaRDD<String> words = langline.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator()).map(BiGramsApp::normalise);

        // Change this part
        JavaRDD<List<String>> pairs = words.m;

        // Count the occurrences of each pair
        JavaPairRDD<List<String>, Integer> counts = pairs.mapToPair(pair -> new Tuple2<>(pair, 1)).reduceByKey((a, b) -> a + b);

        // Sort by descending order of occurrence and take the top 10
        List<Tuple2<List<String>, Integer>> top10 = counts.mapToPair(Tuple2::swap).sortByKey(false).take(10);

        // Save the result
        sparkContext.parallelize(top10).saveAsTextFile(outputDir);


 /*
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
