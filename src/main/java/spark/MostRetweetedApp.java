package spark;

import com.sun.tools.javac.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
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
        /*
        // Filter out tweets without retweet
        JavaRDD<ExtendedSimplifiedTweet> retweets = sentences
                .map(tweet -> ExtendedSimplifiedTweet.fromJson(tweet))
                .filter(tweet -> tweet.isPresent())
                .filter(tweet -> tweet.get().isRetweeted())
                .map(tweet -> tweet.get());

        // Group by user id and calculate total retweet count for each user
        JavaPairRDD<Long, Integer> userRetweets = retweets.mapToPair(tweet -> new Tuple2<Long, Integer>(tweet.getRetweetedUserId(), 1)).reduceByKey((a, b) -> a + b);

        System.out.println(userRetweets.collect().toString());

        // Find the top 10 users with the most retweets
        List<Long> topUsers = userRetweets.mapToPair(Tuple2::swap).sortByKey(false).mapToPair(Tuple2::swap).map(Tuple2::_1).take(10);
        System.out.println(topUsers.toString());

        // Find the most retweeted tweet for each of the top users
        //...
        // Filter out users without tweets in the dataset
        //...
        // save Only show the users from the global top with some tweet in the database (for example: "UserId 4 has 500 retweets, with this tweet having 60 retweets").
        //System.out.println("Total words: " + counts.count());
        //counts.saveAsTextFile(outputDir);


        //for( String x : sentences)
        */

        JavaPairRDD<Long, Long> tweets = sentences
                .map(x -> ExtendedSimplifiedTweet.fromJson(x))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(s -> s.isRetweeted())
                .mapToPair(t -> new Tuple2<Long, Long>(t.getRetweetedUserId(), t.getRetweetedTweetId()));
        //System.out.println(tweets.collect().toString());

        List<Long> mostRetweetedUserId = tweets
                .map(t -> t._1())
                .countByValue()
                .entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .map(s -> s.getKey())
                .collect(Collectors.toList());


        List<String> result = new ArrayList<>();
        for (int i = mostRetweetedUserId.size() - 1; i > mostRetweetedUserId.size() - 11; i--) {
            Long currUser = mostRetweetedUserId.get(i);
            List<Map.Entry<Long, Long>> n = tweets
                    .filter(t -> Objects.equals(t._1, currUser))
                    .map(t -> t._2())
                    .countByValue()
                    .entrySet().stream()
                    .sorted((s1, s2) -> Long.compare(s1.getValue(), s2.getValue()))
                    .collect(Collectors.toList());
            //.max(Comparator.comparingLong(Map.Entry::getValue));
            //if(n.isPresent()) {
            //System.out.println("User: " + currUser + "tweetid: " + n.get().getKey());
            //}

            result.add("User: " + currUser + " Num Retweets: " + n.get(n.size() - 1).getValue() + " Tweet Id: " + n.get(n.size() - 1).getKey());
        }
        System.out.println(result);
    }





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


    private static String normalise(String word) {
        return word.trim().toLowerCase();
    }
}

/*public class MostRetweetedApp {
    public static void main(String[] args) throws IOException {
        String input = args[0];
        String outputDir = args[1];

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("MostRetweetedApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        // Load input
        JavaRDD<String> sentences = sparkContext.textFile(input);
        //for( String x : sentences)
        /*
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


         */



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


    }

    private static String normalise(String word) {
        return word.trim().toLowerCase();
    }
}

 */
