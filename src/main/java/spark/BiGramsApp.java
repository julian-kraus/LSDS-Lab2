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
        System.out.println(language);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("BiGrams");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load input
        JavaRDD<String> line = sparkContext.textFile(input); // creates an RDD of strings, where each string represents a line in the file
        // Filter each line by language we want for one in specific
        List<String> tweets = line.map(t -> ExtendedSimplifiedTweet.fromJson(t)).filter(s -> {
            return s.isPresent() && s.get().getLanguage().equals(language) && !s.get().isRetweeted();
        }).map(s -> s.get().getText()).collect();

        // Split filtered by language lines into words that will be normalized
        //List<String> words = langline.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator()).map(BiGramsApp::normalise).collect();

        Map<String, Integer> biGrams = new HashMap<>();
        System.out.println(tweets.size());
        for (int j = 0; j < tweets.size() - 1; j++) {
            String tweet = tweets.get(j);
            List<String> words = Arrays.stream(tweet.split(" ")).map(BiGramsApp::normalise).collect(Collectors.toList());
            for (int i = 0; i < (words.size() - 2); i++) {
                String str = words.get(i) + " " + words.get(i+1);
                //String[] arr = {words.get(i), words.get(i + 1)};
                if (biGrams.containsKey(str)) {
                    int oldVal= biGrams.get(str);
                    biGrams.replace(str, oldVal + 1);
                    biGrams.put(str, biGrams.get(str) + 1);
                } else {
                    biGrams.put(str, 1);
                }
            }

        }
        List<Map.Entry<String, Integer>> lst = biGrams.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .collect(Collectors.toList());
        System.out.println(lst.get(lst.size()-1));
        for (int i = lst.size()-1; i > lst.size() - 15; i--){

            System.out.println(lst.get(i).getValue() + " " + lst.get(i).getKey());
        }
        //sparkContext.parallelize(lst).saveAsTextFile(outputDir);


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
