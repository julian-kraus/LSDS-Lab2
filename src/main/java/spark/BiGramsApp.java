package spark;

import com.sun.tools.javac.util.Pair;
//import javassist.compiler.ast.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


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
        List<String> tweets = line.filter(s -> {
            Optional<ExtendedSimplifiedTweet> tweet = ExtendedSimplifiedTweet.fromJson(s);
            return tweet.isPresent() && tweet.get().getLanguage().equals(language) && !tweet.get().isRetweeted();
        }).collect();

        // Split filtered by language lines into words that will be normalized
        //List<String> words = langline.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator()).map(BiGramsApp::normalise).collect();

        Map<Pair, Integer> biGrams = new HashMap<>();
        for (int j = 0; j < tweets.size() - 1; j++) {
            String tweet = tweets.get(j);
            List<String> words = Arrays.stream(tweet.split(" ")).map(BiGramsApp::normalise).collect(Collectors.toList());
            for (int i = 0; i < (words.size() - 2); i++) {
                Pair<String, String> p = new Pair<String, String>(words.get(i), words.get(i + 1));
                if (biGrams.containsKey(p)) {
                    biGrams.put(p, biGrams.get(p) + 1);
                } else {
                    biGrams.put(p, 1);
                }
            }
        }
        List<Pair> lst = biGrams.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .limit(10).map(Map.Entry::getKey)
                .collect(Collectors.toList());
        System.out.println(lst.toString());
        sparkContext.parallelize(lst).saveAsTextFile(outputDir);


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
