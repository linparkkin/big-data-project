/**
 * First homework for BDC 17/18
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

public class G18HM1 {

    public static void main(String[] args) throws FileNotFoundException {
    if (args.length == 0) {
      throw new IllegalArgumentException("Expecting the file name on the command line");
    }

    // Read a list of numbers from the program options
    ArrayList<Double> lNumbers = new ArrayList<>();
    Scanner s =  new Scanner(new File(args[0]));
    while (s.hasNext()){
      lNumbers.add(Double.parseDouble(s.next()));
    }
    s.close();

    // Setup Spark
    SparkConf conf = new SparkConf(true)
      .setAppName("Preliminaries");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a parallel collection
    JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);



/**************
 *
 * Absolute value of the difference between each value and the aritmetic mean
 *
 **************/

//    Computing the average of numbers
    double nNumbers = dNumbers.count();
    Double avg = dNumbers.reduce((x, y) -> x + y)/nNumbers;

//    Substracting the average to each number
    JavaRDD<Double> dDiffavgs = dNumbers.map((x) -> Math.abs(x-avg));




/**************
 *
 * Minimum value
 *
 **************/

//    Computing the minimum using the reduce function
    Double lReduceMin = dDiffavgs.reduce((x, y) -> Math.min(x,y));
    System.out.println("Minimum computed with reduce function: " + lReduceMin);

//    Computing the minimum using the min function of JavaRDD
    Double lMin = dDiffavgs.min(new DoubleComparator());
    System.out.println("Minimum with min function: " + lMin);




/**************
 *
 * Computing the relative frequency of each value
 *
 **************/

//    Transform each value x to a pair with key x and value 1
    JavaPairRDD<Double, Integer> dPairs = dNumbers.mapToPair((Double x) -> new Tuple2<>(x, 1));

//    Count the number of elements for each key, in the map the key is the considered element and the value
//    is the number of occurrences
    Map<Double, Long> countMap = dPairs.countByKey();

    System.out.println("Relative Frequencies: ");
//    Get the relative frequency as nOccurrencesValue/nTotElements
    for (Map.Entry <Double, Long> entry : countMap.entrySet()) {
      System.out.println("The relative frequency of the value " + entry.getKey() + " is: " + entry.getValue()/nNumbers);
    }


  }

/**************
*
* Implementing the double comparator
*
**************/

  public static class DoubleComparator implements Serializable, Comparator<Double> {

    @Override
    public int compare(Double a, Double b) {
      if (a < b) return -1;
      else if (a > b) return 1;
      return 0;
    }

  }

}
