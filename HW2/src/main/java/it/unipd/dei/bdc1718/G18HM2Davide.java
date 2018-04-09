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

    //Reading input file and save to cache
    JavaRDD<String> docs = sc.textFile(args[0]).cache().repartition(16);
    long nDocs = docs.count();

    //Counting the total number of words
    long nWords = docs.map(doc -> (doc.split(" ")).length).reduce((x, y) -> x + y);


    /******************************************
     *
     * Simple Word count
     *
     *******************************************/


    JavaPairRDD<String, Long> wordcountDef = docs
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

    /******************************************
     *
     * Improved Word count 1
     *
     *******************************************/

    JavaPairRDD<String, Long> wordCountWC1 = docs
      .flatMapToPair((doc) -> {                                       // <-- Map phase
        String[] tokensArray = doc.split(" ");

        //Empty array containing tuples (word, occurrences)
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

        /*
        For each document count the number of occurrences of every different word.
        The tokensArray is transformed into a stream object that has been grouped by words,
        each group of words is counted using the collector Collectors.counting .
        The result is stored into a map with key the word and value # of occurrences
         */
        Map<String, Long> wordsCount = Arrays.stream(tokensArray).collect(Collectors.groupingBy(Function.identity(),
                Collectors.counting()));

        //Add each entry to pairs array
        for(Map.Entry<String, Long> entry: wordsCount.entrySet()) {
          pairs.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue()));
        }

        return pairs.iterator();

      })
      .groupByKey()                       // <-- Reduce phase
      .mapValues((it) -> {

        //summing the number of occurrences
        long sum = 0;

        for (long c : it) {
          sum += c;
        }

        return sum;

      });


    /******************************************
     *
     * Improved Word count 2
     *
     *******************************************/


    /******* ROUND 1 *******/
    long sqrtN = (long) Math.ceil(Math.sqrt(nWords));
    JavaPairRDD<String, Long> wordCountWC2 = docs
      .flatMapToPair((String doc) -> {                                    // <-- Map phase
        String[] tokensArray = doc.split(" ");
        ArrayList<Tuple2<Long, Tuple2<String,Long>>> pairs = new ArrayList<>();

        /*
        For each document count the number of occurrences of every different word.
        The tokensArray is transformed into a stream object that has been grouped by words,
        each group of words is counted using the collector Collectors.counting .
        The result is stored into a map with key the word and value # of occurrences
        */
        Map<String, Long> wordsCount = Arrays.stream(tokensArray).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        /*
        Produce the intermediate key (x, (word, occurrences)) where x is a random number in
        [0, sqrt(N)) and N is the number of words
         */
        for(Map.Entry<String, Long> entry: wordsCount.entrySet()) {
          pairs.add(new Tuple2<>((long) (Math.floor(Math.random() * sqrtN)),
                  new Tuple2<String, Long>((String) entry.getKey(), (long) entry.getValue())));
        }
        return pairs.iterator();
      })
      .groupByKey()                                     // <-- Reduce phase
      .flatMapToPair((pair) -> {

        //transforming the iterator into a Stream object
        Stream<Tuple2<String, Long>> streamPairs = StreamSupport.stream(pair._2().spliterator(), false);

        /*
        Grouping the intermediate pairs with same key x by words and summing the number of occurrences,
        the result is saved into a map with value (word, occurrencesWithKeyX)
         */
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

      //summing the number of occurrences
      long sum = 0;
      for (long c : it) {
        sum += c;
      }
      return sum;
    });


    /******************************************
     *
     * Word Count with reduceByKey
     *
     *******************************************/

    JavaPairRDD<String, Long> wordCountWCR = docs
      .flatMapToPair((doc) -> {                                             // <-- Map phase
        String[] tokensArray = doc.split(" ");
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

        /*
        For each document count the number of occurrences of every different word.
        The tokensArray is transformed into a stream object that has been grouped by words,
        each group of words is counted using the collector Collectors.counting .
        The result is stored into a map with key the word and value # of occurrences
        */
        Map<String, Long> wordsCount = Arrays.stream(tokensArray).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for(Map.Entry<String, Long> entry: wordsCount.entrySet()) {
          pairs.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue()));
        }
        return pairs.iterator();
      })
      .reduceByKey((x, y) -> x + y);



    /******************************************
     *
     * Performances and Correctness of WC
     *
     *******************************************/

      //Ask the user n words to display
      System.out.println("********* Most occurred words *********");
      Scanner keyboard = new Scanner(System.in);
      System.out.print("Choose the number of most frequent words: ");
      int n= keyboard.nextInt();

      //Word count using definition
      long startWCS = System.currentTimeMillis();
      List<Tuple2<String, Long>> occurrencesWCS= wordcountDef.top(n, new OccurrencesComparator());
      long endWCS = System.currentTimeMillis();

      //Word count improved 1
      long startWC1 = System.currentTimeMillis();
      List<Tuple2<String, Long>> occurrencesWC1= wordCountWC1.top(n, new OccurrencesComparator());
      long endWC1 = System.currentTimeMillis();

      //Word count improved 2
      long startWC2 = System.currentTimeMillis();
      List<Tuple2<String, Long>> occurrencesWC2= wordCountWC2.top(n, new OccurrencesComparator());
      long endWC2 = System.currentTimeMillis();

      //Word count using reduceByKey
      long startWCR = System.currentTimeMillis();
      List<Tuple2<String, Long>> occurrencesWCR= wordCountWCR.top(n, new OccurrencesComparator());
      long endWCR = System.currentTimeMillis();

      //Printing of the n most frequent words and the results about the timing of the different algorithms
      System.out.println("********* MOST FREQUENT WORDS ********");
      System.out.println("Top "+n+" words with straightforward algorithm: " + occurrencesWCS);
      System.out.println("Top "+n+" words with improved word count 1 algorithm: " + occurrencesWC1);
      System.out.println("Top "+n+" words with improved word count 2 algorithm: " + occurrencesWC2);
      System.out.println("Top "+n+" words with improved word count 1 algorithm and reduceByKey: " + occurrencesWCR);

      System.out.println("\n********* TIME PERFORMANCES ********");
      System.out.println("Elapsed with time straightforward algorithm: " + (endWCS - startWCS) + " ms");
      System.out.println("Elapsed with time improved word count 1 algorithm: " + (endWC1 - startWC1) + " ms");
      System.out.println("Elapsed with time improved word count 2 algorithm: " + (endWC2 - startWC2) + " ms");
      System.out.println("Elapsed with time improved word count 1 with reduceByKey method: " + (endWCR - startWCR) + " ms");

      System.out.println("Press Enter to terminate: ");

      try {
          System.in.read();
      } catch (Exception e) {

      }



  }

  //Defining the comparator class for the .top method
  public static class OccurrencesComparator implements Serializable, Comparator<Tuple2<String, Long>>{

    @Override
    public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
      if(o1._2<o2._2)
        return -1;
      if(o1._2==o2._2)
        return  0;
      return 1;
    }

  }

}
