/**
 * Second homework for BDC 17/18
 *
 * Group 18
 *
 * @author  Ala Eddine Ayadi, Giovanni Barbieri, Alessandro Pelizzo, Davide Talon
 *
 */
package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class G18HM2Davide {

  public static void main(String[] args) throws FileNotFoundException {
    if (args.length == 0) {
      throw new IllegalArgumentException("Expecting the file name on the command line");
    }

    // Setup Spark
    SparkConf conf = new SparkConf(true)
            .setAppName("Second homework");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> docs = sc.textFile(args[0]).cache().repartition(16);
    long nItems = docs.count();

    /******************************************
     *
     * Simple Word count
     *
     *******************************************/

    long startDef = System.currentTimeMillis();
    JavaPairRDD<String, Long> wordcountN = docs
        .flatMapToPair((document) -> {             // <-- Map phase
          String[] tokens = document.split(" ");
          ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
          for (String token : tokens) {
            pairs.add(new Tuple2<>(token, 1L));
          }
          return pairs.iterator();
        })
        .groupByKey()                       // <-- Reduce phase
        .mapValues((it) -> {
          long sum = 0;
          for (long c : it) {
            sum += c;
          }
          return sum;
        });

    long endDef = System.currentTimeMillis();
    /******************************************
     *
     * Improved Word count 1
     *
     *******************************************/

    Long startWC1 = System.currentTimeMillis();


    JavaPairRDD<String, Long> wordCountWC1 = docs
            .flatMapToPair((doc) -> {                  // <-- Map phase
              String[] tokensArray = doc.split(" ");
              ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
              Map<String, Long> wordsCount = Arrays.stream(tokensArray).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
              for(Map.Entry<String, Long> entry: wordsCount.entrySet()) {
                pairs.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue()));
              }
              return pairs.iterator();
            })
            .groupByKey()                       // <-- Reduce phase
            .mapValues((it) -> {
              long sum = 0;
              for (long c : it) {
                sum += c;
              }
              return sum;
            });

    Long endWC1 = System.currentTimeMillis();


    /******************************************
     *
     * Improved Word count 2
     *
     *******************************************/

    long startWC2 = System.currentTimeMillis();

    /******* ROUND 1 *******/
    long sqrtN = (long) Math.ceil(Math.sqrt(nItems));
    JavaPairRDD<String, Long> wordCountWC2 = docs
        .flatMapToPair((String doc) -> {                  // <-- Map phase
          String[] tokensArray = doc.split(" ");
          ArrayList<Tuple2<Long, Tuple2<String,Long>>> pairs = new ArrayList<>();
          Map<String, Long> wordsCount = Arrays.stream(tokensArray).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
          for(Map.Entry<String, Long> entry: wordsCount.entrySet()) {
            pairs.add(new Tuple2<>((long) (Math.floor(Math.random() * sqrtN)),
                    new Tuple2<String, Long>((String) entry.getKey(), (long) entry.getValue())));
          }
          return pairs.iterator();
        })
      .groupByKey()                                     // <-- Reduce phase
      .flatMapToPair((pair) -> {

        //counting words summing over different keys
        Stream<Tuple2<String, Long>> streamPairs = StreamSupport.stream(pair._2().spliterator(), false);
        Map<String, Long> wordsCount = streamPairs.collect(Collectors.groupingBy((x) -> x._1(), Collectors.summingLong(Tuple2::_2)));
        ArrayList<Tuple2<String,Long>> wordForKey = new ArrayList<>();
        for(Map.Entry<String, Long> entry: wordsCount.entrySet()) {
          wordForKey.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }
        return wordForKey.iterator();
      });

    /******* ROUND 2 *******/
    wordCountWC2 = wordCountWC2
    .groupByKey()
    .mapValues((it) -> {
      long sum = 0;
      for (long c : it) {
        sum += c;
      }
      return sum;
    });

    long endWC2 = System.currentTimeMillis();



    /******************************************
     *
     * Word Count with reduceByKey
     *
     *******************************************/

    Long startWCR = System.currentTimeMillis();


    JavaPairRDD<String, Long> wordCountWCR = docs
            .flatMapToPair((doc) -> {                  // <-- Map phase
              String[] tokensArray = doc.split(" ");
              ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
              Map<String, Long> wordsCount = Arrays.stream(tokensArray).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
              for(Map.Entry<String, Long> entry: wordsCount.entrySet()) {
                pairs.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue()));
              }
              return pairs.iterator();
            })
            .reduceByKey((x, y) -> x + y);

    Long endWCR = System.currentTimeMillis();


    /******************************************
     *
     * Performances and Correctness of WC
     *
     *******************************************/

    System.out.println("********* Time performances *********");

    System.out.println("Simple WordCount elapsed time: " + (endDef - startDef) + " ms");
    System.out.println("Improved WordCount 1 elapsed time: " + (endWC1 - startWC1) + " ms");
    System.out.println("Improved WordCount 2 elapsed time: " + (endWC2 - startWC2) + " ms");
    System.out.println("Word count with reduceByKey elapsed time: " +(endWCR - startWCR) + " ms");

    System.out.println("********* Most occurred words *********");
    Scanner keyboard = new Scanner(System.in);
    System.out.print("Choose the number of most frequent words: ");
    int n= keyboard.nextInt();

    List<Tuple2<String, Long>> occurrencesWC1= wordCountWC1.top(n, new WordCountComparator());
    List<Tuple2<String, Long>> occurrencesWC2= wordCountWC2.top(n, new WordCountComparator());
    List<Tuple2<String, Long>> occurrencesWCR= wordCountWCR.top(n, new WordCountComparator());

    System.out.println("Improved WordCount 1, most frequent words: " + occurrencesWC1);
    System.out.println("Improved WordCount 2, most frequent words:  " + occurrencesWC2);
    System.out.println("Improved WordCount with reduceByKey, most frequent words: " + occurrencesWCR);

    System.out.println("Press Enter to terminate: ");

    try {
        System.in.read();
    } catch (Exception e) {

    }



  }


  public static class WordCountComparator implements Serializable, Comparator<Tuple2<String, Long>>{

    public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
      if(o1._2<o2._2)
        return -1;
      if(o1._2==o2._2)
        return  0;
      return 1;
    }

  }

}
