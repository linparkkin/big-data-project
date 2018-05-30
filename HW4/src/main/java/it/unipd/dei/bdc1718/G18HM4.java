package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


import java.io.FileNotFoundException;
import java.util.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;



public class G18HM4 {

    private static long startCoreset;
    private static long endCoreset;
    private static long startSeq;
    private static long endSeq;

    public static void main(String[] args) throws FileNotFoundException {

//        Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("MaxDiversity");
        JavaSparkContext sc = new JavaSparkContext(conf);

//      accept command line parameters
        String datafile;
        int k;
        int numBlocks;
        if (args.length == 3) {
          datafile = args[0];
          k = Integer.parseInt(args[1]);
          numBlocks = Integer.parseInt(args[2]);
        } else {
          System.out.println("Wrong number of parameters: you have to specify inputFile k numBlocks");
          throw new IllegalArgumentException();

        }

//        reading and caching the input file
        long startLoad = System.currentTimeMillis();
        JavaRDD<Vector> inputrdd = sc.textFile(datafile).map(InputOutput::strToVector).repartition(numBlocks).cache();
        inputrdd.count();
        long endLoad = System.currentTimeMillis();

//      compute k centers using a coreset of numBlocks*k points obtained with farthest first traversal on each partition
        ArrayList<Vector> pointslist = runMapReduce(inputrdd, k, numBlocks);

//      compute the average distance among points
        double measure = measure(pointslist);



        System.out.println("********** PERFORMANCES **********");

        System.out.println("INFO -- dataset: " + datafile + ", k: " + k + ", numBlocks: " + numBlocks);

        System.out.println("The diversity measure is: "+ measure);
        System.out.println("Time taken by coreset construction: " + (endCoreset - startCoreset) + " ms");
        System.out.println("Time taken by the sequential algorithm for the final solution: " + (endSeq - startSeq) + " ms" );
        System.out.println("Time needed to load and count the file: " + (endLoad - startLoad) + " ms");
        System.out.println("********** END **********");

    }

/**
 *compute k centers using a coreset of numBlocks*k points obtained with farthest first traversal on each partition
 */
    public static ArrayList<Vector> runMapReduce(JavaRDD<Vector> pointsrdd, int k, int numBlocks){

//      partitions pointsrdd into numBlocks chunks
        startCoreset = System.currentTimeMillis();
        JavaRDD partitions = pointsrdd.repartition(numBlocks);

//      for each partition create extract k points
        JavaRDD coreset = partitions.mapPartitions(new FlatMapFunction<Iterator<Vector>, Vector>() {
              @Override
              public Iterator call(Iterator<Vector> VectorsOfPartition) throws Exception {
                    ArrayList<Vector> P = new ArrayList<Vector>();

//                  add each point of the partition to an ArrayList P
                    while(VectorsOfPartition.hasNext()){
                        Vector v = VectorsOfPartition.next();
                        P.add(v);
                    }

                    ArrayList<Vector> points = kcenter(P, k);

                    return points.iterator();

              }
        });

//      collect numBlocks * k centers as coreset
        ArrayList<Vector> coresetArrayList = new ArrayList<>();
        coresetArrayList.addAll(coreset.collect());
        endCoreset = System.currentTimeMillis();

//      extract k centers using runSequential algorithm
        startSeq = System.currentTimeMillis();
        ArrayList<Vector> maxDiversity = runSequential( coresetArrayList, k);
        endSeq = System.currentTimeMillis();

        return maxDiversity;
    }

/**
 * Compute the average distance between all possible couples of points.
 */
    public static double measure(ArrayList<Vector> pointslist){
        /*
        average distance between all points in pointslist
        (i.e., the sum of all pairwise distances divided by the number of distinct pairs)
         */

        //The number of distinct pairs can be computed as the number of combinations C(n,k), where n is the size of pointslist
        //and k is two.
        long nPoints=(long) pointslist.size();
        long distinctPairs=(nPoints-1)*(nPoints)/2l;
        long avgDistance=0l;

        for(int i=0; i<nPoints-1;i++){
            for (int j=i+1; j<nPoints;j++){
                avgDistance+=Vectors.sqdist(pointslist.get(i),pointslist.get(j));
            }
        }

        return (double) avgDistance/distinctPairs;

  }


/**
* Sequential approximation algorithm based on matching.
*/
    public static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k) {

        final int n = points.size();
        if (k >= n) {
            return points;
        }

        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter=0; iter<k/2; iter++) {
//              Find the maximum distance pair among the candidates
              double maxDist = 0;
              int maxI = 0;
              int maxJ = 0;
              for (int i = 0; i < n; i++) {
                    if (candidates[i]) {
                          for (int j = i+1; j < n; j++) {
                                if (candidates[j]) {
                                    double d = Math.sqrt(Vectors.sqdist(points.get(i), points.get(j)));
                                    if (d > maxDist) {
                                        maxDist = d;
                                        maxI = i;
                                        maxJ = j;
                                    }
                                }
                          }
                    }
              }
//               Add the points maximizing the distance to the solution
              result.add(points.get(maxI));
              result.add(points.get(maxJ));
//               Remove them from the set of candidates
              candidates[maxI] = false;
              candidates[maxJ] = false;
        }
//         Add an arbitrary point to the solution, if k is odd.
        if (k % 2 != 0) {
              for (int i = 0; i < n; i++) {
                    if (candidates[i]) {
                          result.add(points.get(i));
                          break;
                    }
              }
        }
        if (result.size() != k) {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;
    }


/**
 * Compute the farthest first trasversal.
 */
      private static ArrayList<Vector> kcenter(ArrayList<Vector> P, int k){

          ArrayList <Vector> centers= new ArrayList<>();
//          Assign the first element of the ArrayList as first arbitrary center c1 and remove it from the list of points;
          centers.add(P.get(0));
          P.remove(0);
//          Define an ArrayList of double which will contains the minimum distance of a point from the set S (set of centers)
          ArrayList< Double > distances = new ArrayList<>(Collections.nCopies(P.size(), Double.MAX_VALUE));
//          Start the Farthest-first traversal algorithm
          for (int i = 0; i < k-1; i++) {
//              Define a neutral maximum distance from the set S and a neutral index
              int indexMaxPoint = -1;
              double max = -1;
//              Iterate among all the points of the dataset
              for (int j = 0; j < P.size(); j++) {
//                  Compute the distance between the point j and the center i
                  double distance= Vectors.sqdist(centers.get(i), P.get(j));

//                  If the distance is lower than the one on the ArrayList, update it
                  if(distances.get(j) > distance)
                      distances.set(j,Vectors.sqdist(centers.get(i), P.get(j)));
//                  If the distance is greater than the maximum distance from the set S, set this distance as the new max
                  if(distances.get(j)>max){
                      max= distances.get(j);
                      indexMaxPoint=j;
                  }
            }
//            Add the point with the maximum distance to the ArrayList centers, and remove it from the
//             distances ArrayList and from the set of point P
            centers.add(P.get(indexMaxPoint));
            distances.remove(indexMaxPoint);
            P.remove(indexMaxPoint);
          }


//          add to the list of points the selected centers
          for(Vector c : centers) {
              P.add(c);
          }

//          Return the set of centers
          return centers;

      }

}
