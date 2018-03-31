/**
 * Second homework for BDC 17/18
 *
 * Group 18
 *
 * @author  Ala Eddine Ayadi, Giovanni Barbieri, Alessandro Pelizzo, Davide Talon
 *
 */
package it.unipd.dei.bdc1718;

import org.apache.parquet.it.unimi.dsi.fastutil.doubles.DoubleComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.Scanner;
import java.util.function.BiConsumer;

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
    JavaRDD<String> docs = sc.textFile(args[0]);

    /***********************************************************
    *
    * FULL EXAMPLE FROM THE SITE(COUNTING WORDS)
    *
    ************************************************************/

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


    //Swap key and value
    JavaPairRDD<Long, String> swappedWordCounts= wordcounts.mapToPair(x->x.swap());

    //Sort the JavaPairRDD by key in descending order
    swappedWordCounts.sortByKey(false);

    //The user can choose the number n of most frequent words he wants to see
    Scanner keyboard = new Scanner(System.in);
    System.out.print("Choose the number of most frequent words: ");
    int n= keyboard.nextInt();

    //Printing of the n most frequent words
    System.out.println("Top "+n+" words with straightforward algorithm:");
    System.out.println(swappedWordCounts.top(n, new LongTupleComparator()));


    }


//Defining the comparator class for the .top method

    public static class LongTupleComparator implements Serializable, Comparator<Tuple2<Long,String>> {

        @Override
        public int compare(Tuple2<Long,String> a, Tuple2<Long,String> b ) {
            if (a._1 < b._1) return -1;
            else if (a._1 > b._1) return 1;
            return 0;
        }
    }

}

