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
import java.util.List;
import java.util.Scanner;

public class G18HM2 {

    public static void main(String[] args) throws IllegalArgumentException {

        if (args.length == 0) {
          throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        // Setup Spark
        SparkConf conf = new SparkConf(true)
          .setAppName("Second Homework");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Import the text-sample.txt document
        JavaRDD<String> docs = sc.textFile(args[0]).cache().repartition(16);
        long wordsNumber = docs.count();

        /***********************************************************
        *
        * SIMPLE WORD COUNT
        *
        ************************************************************/

        //Start
        long startWCS = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcounts = docs
            .flatMapToPair((document) -> {                                              // <-- Map phase
                String[] tokens = document.split(" ");
                ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                //for each token add the pair (word, 1)
                for (String token : tokens) {
                    pairs.add(new Tuple2<>(token, 1L));
                }
                return pairs.iterator();
            })
            .groupByKey()                                                               // <-- Reduce phase
            .mapValues((it) -> {
                long sum = 0;
                for (long c : it) {
                    sum += c;
                }
                return sum;
            });

        //End
        long endWCS = System.currentTimeMillis();



        /***********************************************************
         *
         * IMPROVED WORD COUNT 1
         *
         ************************************************************/

        //Start
        long startWC1 = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcounts1 = docs
                .flatMapToPair((document) -> {                                          // <-- Map phase
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    //Defining an empty ArrayList of String. This list will be filled step by step
                    //with the counted words in order to avoid that a word is counted multiple times.
                    //The check is done with the method alreadyChecked() .
                    ArrayList<String> checkAL=new ArrayList<>();
                    long count;
                    for (String token : tokens) {

                        //Check if the word has not been already counted
                        if (alreadyChecked((token),checkAL)==false) {

                            // Count the number of occurrences of a given word in the document
                            count = numberOfOccurences(token, tokens);
                            pairs.add(new Tuple2<>(token, count));
                            checkAL.add(token);
                        }
                    }
                    return pairs.iterator();
                })
                .groupByKey()                                                           // <-- Reduce phase
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });

        //End
        long endWC1 = System.currentTimeMillis();


        /***********************************************************
         *
         * IMPROVED WORD COUNT 2
         *
         ************************************************************/

        /***Round 1***/
        long startWC2 = System.currentTimeMillis();

        //Compute the square root of the overall number of words.
        long sqrtN= (long) Math.sqrt(wordsNumber);

        //In order to implement the "improved word count 2" algorithm it is necessary to define an object that better represent
        //the pair (x,(w,c)). This is done creating a JavaPairRDD<Long, Tuple2<String, Long>> Object.
        JavaPairRDD<String, Long> wordcounts2 = docs
                .flatMapToPair((document) -> {                                              // <-- Map phase
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<Long, Tuple2<String,Long>>> pairs = new ArrayList<>();

                    //Defining an empty ArrayList of String. This list will be filled step by step with
                    //the counted words in order to avoid that a word is counted multiple times.
                    //The check is done with the method alreadyChecked() .
                    ArrayList<String> checkAL=new ArrayList<>();
                    long count;
                    for (String token : tokens) {

                        //Check if the word has not been already counted
                        if (alreadyChecked(token,checkAL)==false) {

                            // Count the number of occurrences of a given word in the document
                            count = numberOfOccurences(token, tokens);

                            // Assigning the number x, which is a random number between [0, sqrt(N)), as the key of the
                            // JavaPairRDD<Long, Tuple2<String, Long>> object.
                            long x = (long) (Math.random()*sqrtN) ;
                            pairs.add(new Tuple2<>(x, new Tuple2<String, Long>(token, count)));
                            checkAL.add(token);

                        }
                    }

                    return pairs.iterator();
                }).groupByKey()                                                               // <-- Reduce phase
                .flatMapToPair((it) -> {
                    long count;
                    ArrayList<Tuple2<String,Long>> newPairs= new ArrayList<>();

                    //Defining an empty ArrayList of String. This list will be filled step by step
                    // with the counted words in order to avoid that a word is counted multiple times.
                    // The check is done with the method alreadyChecked() .
                    ArrayList<String> checkAL=new ArrayList<>();
                    for (Tuple2<String, Long> pair : it._2) {
                        if (alreadyChecked(pair._1,checkAL)==false){

                            // Count the number of occurrences of a given word according to its key.
                            count=numberOfOccurencesOn2Tuple(pair , it._2);
                            newPairs.add(new Tuple2<>(pair._1,count));
                            checkAL.add(pair._1);
                        }
                    }
                    return newPairs.iterator();
                });


        /***Round 2***/
        //The map phase apply the identity function, so nothing changes                        <-- Map phase
        wordcounts2= wordcounts2.groupByKey()                                           // <-- Reduce phase

                //Count the number of occurrences of a word summing its partial-occurrences
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });

        long endWC2 = System.currentTimeMillis();



    /***********************************************************
     *
     * IMPROVED WORD COUNT 1 WITH reduceByKey METHOD
     *
     ************************************************************/

        long startWCR = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcountsR = docs
                .flatMapToPair((document) -> {                                               // <-- Map phase
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    //Defining an empty ArrayList of String. This list will be filled step by step
                    //with the counted words in order to avoid that a word is counted multiple times.
                    // The check is done with the method alreadyChecked() .
                    ArrayList<String> checkAL=new ArrayList<>();
                    long count;
                    for (String token : tokens) {

                        //Check that the word has not been already counted
                        if (alreadyChecked((token),checkAL)==false) {
                            count = numberOfOccurences(token, tokens);
                            pairs.add(new Tuple2<>(token, count));
                            checkAL.add(token);
                        }
                    }
                    return pairs.iterator();
                })

                //Applying the reduce function summing the values for each key
                .reduceByKey((x,y) -> x+y);                                                  // <-- Reduce phase

        long endWCR = System.currentTimeMillis();


        /***********************************************************
         *
         * PERFORMANCES AND CORRECTNESS
         *
         ************************************************************/

        //The user can choose the number n of most frequent words he wants to see
        Scanner keyboard = new Scanner(System.in);
        System.out.print("Choose the number of most frequent words: ");
        int n= keyboard.nextInt();

        //Computing the most n words with each algorithm
        List<Tuple2<String, Long>> occurrencesWCS = wordcounts.top(n, new LongTupleComparator());
        List<Tuple2<String, Long>> occurrencesWC1 = wordcounts1.top(n, new LongTupleComparator());
        List<Tuple2<String, Long>> occurrencesWC2 = wordcounts2.top(n, new LongTupleComparator());
        List<Tuple2<String, Long>> occurrencesWCR = wordcountsR.top(n, new LongTupleComparator());


        //Printing of the n most frequent words and the results about the timing of the different algorithms
        System.out.println("********* MOST FREQUENT WORDS ********");
        System.out.println("Top "+n+" words with straightforward algorithm: " + occurrencesWCS);
        System.out.println("Top "+n+" words with improved word count 1 algorithm: " + occurrencesWC1);
//        System.out.println("Top "+n+" words with improved word count 2 algorithm: " + occurrencesWC2);
        System.out.println("Top "+n+" words with improved word count 1 algorithm and reduceByKey: " + occurrencesWCR);

        System.out.println("********* TIME PERFORMANCES ********");
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

