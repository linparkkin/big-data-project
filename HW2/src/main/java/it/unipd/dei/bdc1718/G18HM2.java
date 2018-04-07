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

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Scanner;

public class G18HM2 {

    public static void main(String[] args) throws FileNotFoundException {
    if (args.length == 0) {
      throw new IllegalArgumentException("Expecting the file name on the command line");
    }

    // Setup Spark
    SparkConf conf = new SparkConf(true)
      .setAppName("PreliminariesTwo");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a parallel collection
    JavaRDD<String> docs = sc.textFile(args[0]).cache().repartition(16);
    docs.count();

    /***********************************************************
    *
    * WORD COUNT
    *
    ************************************************************/

    long start1 = System.currentTimeMillis();
    JavaPairRDD<String, Long> wordcounts = docs
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
    long end1 = System.currentTimeMillis();

    //Number of words
    long wordsNumber= wordcounts.count();



    /***********************************************************
     *
     * IMPROVED WORD COUNT 1
     *
     ************************************************************/

    long start2 = System.currentTimeMillis();
    JavaPairRDD<String, Long> wordcounts1 = docs
            .flatMapToPair((document) -> {                                 // <-- Map phase
                String[] tokens = document.split(" ");
                ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                ArrayList<String> checkAL=new ArrayList<>();
                long count;
                for (String token : tokens) {
                    if (alreadyChecked((token),checkAL)==false) {
                        count = numberOfOccurences(token, tokens);
                        pairs.add(new Tuple2<>(token, count));
                        checkAL.add(token);
                    }
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

        long end2 = System.currentTimeMillis();


    /***********************************************************
     *
     * IMPROVED WORD COUNT 2
     *
     ************************************************************/

    //Round 1
    long start3 = System.currentTimeMillis();
    long sqrtN= (long) Math.sqrt(wordsNumber);
    JavaPairRDD<Long, Tuple2<String, Long>> wordcounts2 = docs
            .flatMapToPair((document) -> {                                 // <-- Map phase
                String[] tokens = document.split(" ");
               ArrayList<Tuple2<Long,Tuple2<String, Long>>> pairs = new ArrayList<>();
                ArrayList<String> checkAL=new ArrayList<>();
                long count;
                for (String token : tokens) {
                    if (alreadyChecked(token,checkAL)==false) {
                        count = numberOfOccurences(token, tokens);
                        long x = (long) (Math.random()*sqrtN) ;
                        pairs.add(new Tuple2<>(x, new Tuple2<>(token,count)));
                        checkAL.add(token);
                    }
                }
                return pairs.iterator();
            });

    JavaPairRDD<String, Long> redwordcount2= wordcounts2
            .groupByKey()                       // <-- Reduce phase
            .flatMapToPair((it) -> {
                long count;
                ArrayList<String> checkAL=new ArrayList<>();
                ArrayList<Tuple2<String,Long>> newPairs= new ArrayList<>();
                for (Tuple2<String, Long> pair : it._2) {
                    if (alreadyChecked(pair._1,checkAL)==false){
                        count=numberOfOccurencesOn2Tuple(pair , it._2);
                        newPairs.add(new Tuple2<>(pair._1,count));
                        checkAL.add(pair._1);
                    }
                }
                return newPairs.iterator();
            });

        //Round 2
    //The map phase apply the identity function, so nothing changes                    <-- Map phase
    redwordcount2= redwordcount2.groupByKey()                                       // <-- Reduce phase
            .mapValues((it) -> {
                long sum = 0;
                for (long c : it) {
                    sum += c;
                }
                return sum;
            });

    long end3 = System.currentTimeMillis();



    /***********************************************************
     *
     * IMPROVED WORD COUNT 1
     *
     ************************************************************/

    long start4 = System.currentTimeMillis();
    JavaPairRDD<String, Long> wordcounts4 = docs
            .flatMapToPair((document) -> {                                 // <-- Map phase
                String[] tokens = document.split(" ");
                ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                ArrayList<String> checkAL=new ArrayList<>();
                long count;
                for (String token : tokens) {
                    if (alreadyChecked((token),checkAL)==false) {
                        count = numberOfOccurences(token, tokens);
                        pairs.add(new Tuple2<>(token, count));
                        checkAL.add(token);
                    }
                }
                return pairs.iterator();
            })
            .reduceByKey((x,y) -> x+y);

    long end4 = System.currentTimeMillis();


    /***********************************************************
     *
     * PRINTINGS
     *
     ************************************************************/

    //The user can choose the number n of most frequent words he wants to see
    Scanner keyboard = new Scanner(System.in);
    System.out.print("Choose the number of most frequent words: ");
    int n= keyboard.nextInt();

    //Printing of the n most frequent words and the results about the timing of the different algorithms
    System.out.println("Top "+n+" words with straightforward algorithm:");
    System.out.println(wordcounts.top(n, new LongTupleComparator()));
    System.out.println("Top "+n+" words with improved word count 1 algorithm:");
    System.out.println(wordcounts1.top(n, new LongTupleComparator()));
    System.out.println("Top "+n+" words with improved word count 2 algorithm:");
    System.out.println(redwordcount2.top(n, new LongTupleComparator()));
    System.out.println("Top "+n+" words with improved word count 1 algorithm and reduceByKey:");
    System.out.println(redwordcount2.top(n, new LongTupleComparator()));
    System.out.println("Elapsed time straightforward algorithm: " + (end1 - start1) + " ms");
    System.out.println("Elapsed time improved word count 1 algorithm: " + (end2 - start2) + " ms");
    System.out.println("Elapsed time improved word count 2 algorithm: " + (end3 - start3) + " ms");
    System.out.println("Elapsed time improved word count 1 with reduceByKey method: " + (end4 - start4) + " ms");
    }


//Defining the comparator class for the .top method

    public static class LongTupleComparator implements Serializable, Comparator<Tuple2<String,Long>> {

        @Override
        public int compare(Tuple2<String,Long> a, Tuple2<String,Long> b ) {
            if (a._2 < b._2) return -1;
            else if (a._2 > b._2) return 1;
            return 0;
        }
    }

//Defining a method to count the number of occurences of a word w inside a document
    private static long numberOfOccurences (String a, String[] b){
       long count =0;
        for (String c:b) {
            if(a.equals(c)){
                count++;
            }
        }
        return count;
    }

//Defining a method to check if the word of a document has already been counted
    private static Boolean alreadyChecked (String a, ArrayList<String> b){
        Boolean checked =false;
        for (String c:b) {
            if(a.equals(c)){
                checked=true;
            }
        }
        return checked;
}

//Defining a method to count the number of occurences of a word w from a list of Tuple2
    private static long numberOfOccurencesOn2Tuple (Tuple2<String,Long> a, Iterable<Tuple2<String,Long>> b){
        long count = 0;
        for (Tuple2<String,Long> c:b) {
            if(a._1.equals(c._1)){
                count+=c._2;
            }
        }
        return count;
    }

}

