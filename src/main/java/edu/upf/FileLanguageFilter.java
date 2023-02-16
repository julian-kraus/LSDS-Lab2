package edu.upf;

import com.google.gson.Gson;
import edu.upf.model.SimplifiedTweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import spark.ExtendedSimplifiedTweet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.Optional;


public class FileLanguageFilter {
    final String inputFile;
    final String outputFile;

    public FileLanguageFilter(String inputFile, String outputFile) {
            this.inputFile = inputFile;
            this.outputFile = outputFile;
        }

    public void filterLanguage(String language) throws Exception {
            try{

            Gson gson = new Gson();
            /* read file
            BufferedReader br = new BufferedReader(new FileReader(inputFile));  
            
            // initialization
            String line;  
            BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true)); // where we will write the accepted (line by line)
            
            // List<SimplifiedTweet> lTweets = Files.lines(Paths.get(fileName))).forEach(l -> SimplifiedTweet.fromJson(s)).filter(t -> t.isPresent() && t.get().getLanguage().equals(language).collect(Collectors.toList());
            while ((line = br.readLine()) != null)  // while not end string character 
            {  
                // converting line to SimplifiedTweet
                Optional<SimplifiedTweet> tweet = SimplifiedTweet.fromJson(line);
                // if tweet not empty and correct language add it to lTweets
                if (tweet.isPresent() && tweet.get().getLanguage().equals(language)) {
                    gson.toJson(tweet.get(), bw); // write
                    bw.newLine();
                }
            }
             br.close(); // close files
            bw.close();
             */
                BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true)); // where we will write the accepted (line by line)
                SparkConf conf = new SparkConf().setAppName("Word Count");
                JavaSparkContext sparkContext = new JavaSparkContext(conf);
                // Load input
                JavaRDD<String> sentences = sparkContext.textFile(this.inputFile);
                List<String> tweets = sentences.collect();
                for(String line: tweets) {
                    // converting line to SimplifiedTweet
                    Optional<SimplifiedTweet> tweet = SimplifiedTweet.fromJson(line);
                    // if tweet not empty and correct language add it to lTweets
                    if (tweet.isPresent() && tweet.get().getLanguage().equals(language)) {
                        gson.toJson(tweet.get(), bw); // write
                        bw.newLine();
                    }
                }
                bw.close();

        
        } catch(Exception e){
            e.printStackTrace();
            throw e; 
            }
            
    }
}

// mvn clean package before submission if not not compiling and then not graded