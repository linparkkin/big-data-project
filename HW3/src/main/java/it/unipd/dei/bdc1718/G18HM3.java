package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class G18HM3 {

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

    //run kcenter and print the running time
    System.out.println("**************** Running kcenter ****************");
    long startKCenters = System.currentTimeMillis();

    long endKCenters = System.currentTimeMillis();
    System.out.println("Time elapsed for kcenters: " + (endKCenters - startKCenters));



    //run kmeansPP
    System.out.println("**************** Running kmeansPP ****************");

    long startKMeansPP = System.currentTimeMillis();
    ArrayList<Vector> centers = kmeansPP(P, weights, k);
    long endKMeansPP = System.currentTimeMillis();

    long startKMeansObj = System.currentTimeMillis();
    double distance = kmeansObj(P, centers);
    long endKMeansObj = System.currentTimeMillis();

    System.out.println("Mean distance of P: " +  distance);
    System.out.println("Time elapsed for kmeansPP: " + (endKMeansPP - startKMeansPP));
    System.out.println("Time elapsed for kmeansObj: " + (endKMeansObj - startKMeansObj));


    //test the coreset
    System.out.println("**************** Test coreset performances ****************");

    ArrayList<Vector> coreset = kcenter(P, k1);
    ArrayList<Vector> CoresetCenters = kmeansPP(P, weights, k);
    double distanceCoreset = kmeansObj(P, CoresetCenters);
    System.out.println("Mean distance of P using coreset: " +  distanceCoreset);



  }

  public static ArrayList<Vector> kcenter (ArrayList<Vector> P, int k) {
    return P;
  }

/*
weighted variant of the kmeans++ algorithm where, in each iteration, the probability for a non-center point
p of being chosen as next center is w_p*(d_p)^2/(sum_{q non center} w_q*(d_q)^2)
*/
  public static ArrayList<Vector> kmeansPP (ArrayList<Vector> P, ArrayList<Long> WP, int k) {

    //array of selected centers
    ArrayList<Vector> centers = new ArrayList<>(k);

    ArrayList<Double> dFromClosestCenter = new ArrayList<>(Collections.nCopies(P.size(), Double.MAX_VALUE));
    ArrayList<Integer> closestCenter = new ArrayList<>(P.size());

    ArrayList<Double> pointsProb = new ArrayList<>(P.size());


    //get a random integer in [0, |P|)
    int firstCenterIndex = (int) Math.floor(Math.random()* P.size());

    centers.add(P.get(firstCenterIndex));

    for(int iCenter = 0; iCenter < k; iCenter++){

      double totalwpdp = 0;
      for(int iPoint = 0; iPoint < P.size(); iPoint++){

//        compute the distance between the point and the currentCenter
        double distance = Vectors.sqdist(P.get(iCenter), P.get(iPoint));

//        if the current center is closer, set it as the closest center for the point
        if (distance < dFromClosestCenter.get(iPoint)){
          dFromClosestCenter.set(iPoint, distance);
          closestCenter.set(iPoint, iCenter);
          double wpdp = WP.get(iPoint) * dFromClosestCenter.get(iPoint);
          pointsProb.set(iPoint, wpdp);
        }

        totalwpdp += pointsProb.get(iPoint);

      }

      double rnd = Math.random();

      double cumProb = 0;
      boolean centerFound = false;

      //centers have 0 probability of been selected (distance 0),
      // supposing k = o(N) we can avoid the check for already selected centers
      for (int iPoint = 0; iPoint < P.size() && !centerFound; iPoint++) {
        double pointProb = pointsProb.get(iPoint)/ totalwpdp;

        cumProb += pointProb;

        if (rnd <= cumProb) {
          centerFound = true;
          centers.add(P.get(iCenter));
        }
      }

    }

    return centers;

  }

/*
compute the verage squared distance of a point of P from its closest center
(i.e., the kmeans objective function for P with centers C, divided by the number of points of P)
*/
  public static double kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C) {


    double totDist = 0;
    for (int iPoint = 0; iPoint < P.size(); iPoint++) {

      double pointDistance = Double.MAX_VALUE;

//      find the closest center for the point
      for (int iCenter = 0; iCenter < C.size(); iCenter++) {

        double distCurrCenter = Vectors.sqdist(P.get(iCenter), P.get(iPoint));
        if (distCurrCenter < pointDistance) {
          pointDistance = distCurrCenter;
        }

      }

//    compute the total distance
      totDist += pointDistance;

    }

    return totDist / P.size();

  }



}
