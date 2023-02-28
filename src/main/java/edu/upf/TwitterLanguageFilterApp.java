package edu.upf;

import edu.upf.uploader.S3Uploader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TwitterLanguageFilterApp {
    public static void main( String[] args ) throws Exception {
        long startTime = System.nanoTime();
        //Create a SparkContext to initialize
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String bucket = argsList.get(2);
        //System.out.println("Language: " + language + ". Output file: " + outputFile + ". Destination bucket: " + bucket);
        for(String inputFile: argsList.subList(3, argsList.size())) {
            System.out.println("Processing: " + inputFile);
            final FileLanguageFilter filter = new FileLanguageFilter(inputFile, outputFile);
            filter.filterLanguage(language);
        }

        final S3Uploader uploader = new S3Uploader(bucket, language, "upf");
        uploader.upload(Arrays.asList(outputFile));

        //Path path = Paths.get(outputFile);
        /*long lines = 0;
        lines= Files.lines(path).parallel().count();
        System.out.println(lines + "lines are in this outputfile.");

        /
        /System.out.println(System.nanoTime()-startTime);

         */


    }
}
