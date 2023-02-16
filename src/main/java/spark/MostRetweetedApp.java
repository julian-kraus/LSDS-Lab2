package spark;

import com.sun.tools.javac.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MostRetweetedApp {
    public static void main(String[] args) throws IOException {
        String input = args[0];
        String outputDir = args[1];

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("MostRetweetedApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        // Load input
        JavaRDD<String> sentences = sparkContext.textFile(input);
        //for( String x : sentences)

        JavaPairRDD<Long, Long> tweets = sentences
                .map(x -> ExtendedSimplifiedTweet.fromJson(x))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(s -> s.isRetweeted())
                .mapToPair(t -> new Tuple2<Long,Long>(t.getRetweetedUserID(),t.getRetweetedTweetID()));

        List<Long> mostRetweetedUserId = tweets
                .map(t -> t._1())
                .countByValue()
                .entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .map(s -> s.getKey())
                .collect(Collectors.toList())
                .subList(0, 9);




        // in list Pair of RetweetedUserId and his retweetedTweet
        /*for(String tweet: tweets) {
            Optional<ExtendedSimplifiedTweet> prelimTweet = ExtendedSimplifiedTweet.fromJson(tweet);
            if (prelimTweet.isPresent()) {
                String text = prelimTweet.get().getText();
                System.out.println(text + " nextext ");
            }
        }

         */


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
}
