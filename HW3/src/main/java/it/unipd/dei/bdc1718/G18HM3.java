/**
 * Third homework for BDC 17/18
 *
 * Group 18
 *
 * @author  Ala Eddine Ayadi, Giovanni Barbieri, Alessandro Pelizzo, Davide Talon
 *
 */
package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.io.IOException;
import java.util.*;


public class G18HM3 {

  public static void main(String[] args) throws IOException {

      String inputFile;
      int k;
      int k1;
      if (args.length == 3) {
          inputFile = args[0];
          k = Integer.parseInt(args[1]);
          k1 = Integer.parseInt(args[2]);

          if (k >= k1) {
              System.out.println("Parameters not valid: must be k < k1!");
              throw new IllegalArgumentException();
          }

      } else {
          System.out.println("Wrong number of parameters: you have to specify inputFile k k1");
          throw new IllegalArgumentException();

      }
      ArrayList<Vector> P = InputOutput.readVectorsSeq(inputFile);

      //run kcenter and print the running time
      System.out.println("**************** Running kcenter ****************");
      long startKCenters = System.currentTimeMillis();
      //kcenter is slower than kcenterPP probably for cache missing
      ArrayList<Vector> kcenters = kcenter(P, k);
      long endKCenters = System.currentTimeMillis();
      System.out.println("Time elapsed for kcenters: " + (endKCenters - startKCenters) + " ms");



      //run kmeansPP
      ArrayList<Long> weights = new ArrayList<>(Collections.nCopies(P.size(), 1L));
      System.out.println("**************** Running kmeansPP ****************");

      long startKMeansPP = System.currentTimeMillis();
      ArrayList<Vector> centers = kmeansPP(P, weights, k);
      long endKMeansPP = System.currentTimeMillis();

      long startKMeansObj = System.currentTimeMillis();
      double distance = kmeansObj(P, centers);
      long endKMeansObj = System.currentTimeMillis();

      System.out.println("Average squared distance of P: " +  distance);
      System.out.println("Time elapsed for kmeansPP: " + (endKMeansPP - startKMeansPP) + " ms");
      System.out.println("Time elapsed for kmeansObj: " + (endKMeansObj - startKMeansObj) + " ms");


      //test the coreset
      System.out.println("**************** Test coreset performances ****************");

      ArrayList<Vector> coreset = kcenter(P, k1);

      ArrayList<Vector> coresetCentersW1 = kmeansPP(coreset, weights, k);

      ArrayList<Long> wx = computeCoresetWeights(P, coreset);
      ArrayList<Vector> coresetCentersWX = kmeansPP(coreset, wx, k);

      double distanceCoresetW1 = kmeansObj(P, coresetCentersW1);
      System.out.println("Average squared distance of P using coreset (weights equal to 1): " +  distanceCoresetW1);

      double distanceCoresetWX = kmeansObj(P, coresetCentersWX);
      System.out.println("Average squared distance of P using coreset (weights equal to the number of points of the corresponding cluster): " +  distanceCoresetWX);


  }

    private static ArrayList<Vector> kcenter(ArrayList<Vector> P, int k){

        ArrayList <Vector> centers= new ArrayList<>();
        //Assign the first element of the ArrayList as first arbitrary center c1 and remove it from the list of points;
        centers.add(P.get(0));
        P.remove(0);
        //Define an ArrayList of double which will contains the minimum distance of a point from the set S (set of centers)
        ArrayList< Double > distances = new ArrayList<>(Collections.nCopies(P.size(), Double.MAX_VALUE));
        //Start the Farthest-first traversal algorithm
        for (int i = 0; i < k-1; i++) {
          //Define a neutral maximum distance from the set S and a neutral index
          int indexMaxPoint = -1;
          double max = -1;
          //Iterate among all the points of the dataset
          for (int j = 0; j < P.size(); j++) {
            //Compute the distance between the point j and the center i
            double distance= Vectors.sqdist(centers.get(i), P.get(j));

            //If the distance is lower than the one on the ArrayList, update it
              if(distances.get(j) > distance)
                distances.set(j,Vectors.sqdist(centers.get(i), P.get(j)));
              //If the distance is greater than the maximum distance from the set S, set this distance as the new max
              if(distances.get(j)>max){
                max= distances.get(j);
                indexMaxPoint=j;
              }
          }
          //Add the point with the maximum distance to the ArrayList centers, and remove it from the distances ArrayList and from the set of point P
          centers.add(P.get(indexMaxPoint));
          distances.remove(indexMaxPoint);
          P.remove(indexMaxPoint);
        }


        //add to the list of points the selected centers
        for(Vector c : centers) {
            P.add(c);
        }

        //Return the set of centers
        return centers;

    }


    /*
  weighted variant of the kmeans++ algorithm where, in each iteration, the probability for a non-center point
  p of being chosen as next center is w_p*(d_p)^2/(sum_{q non center} w_q*(d_q)^2)
  */
    public static ArrayList<Vector> kmeansPP (ArrayList<Vector> P, ArrayList<Long> WP, int k) {

        //array of selected centers
        ArrayList<Vector> centers = new ArrayList<>(k);

        //array of distances of points from their closest center
        ArrayList<Double> dFromClosestCenter = new ArrayList<>(Collections.nCopies(P.size(), Double.MAX_VALUE));

        //array of closest centers
        ArrayList<Integer> closestCenter = new ArrayList<>(Collections.nCopies(P.size(), 0));

        //array of points probability
        ArrayList<Double> pointsProb = new ArrayList<>(Collections.nCopies(P.size(), (double) 0));


        //get a random integer in [0, |P|)
        int firstCenterIndex = (int) Math.floor(Math.random()* P.size());

        //add the chosen point to the list of centers
        centers.add(P.get(firstCenterIndex));

        //remove the point from P
        P.remove(firstCenterIndex);
        dFromClosestCenter.remove(firstCenterIndex);
        closestCenter.remove(firstCenterIndex);
        pointsProb.remove(firstCenterIndex);


        for(int iCenter = 0; iCenter < k; iCenter++){

            double totalwpdp = 0;
            for(int iPoint = 0; iPoint < P.size(); iPoint++){

//        compute the distance between the point and the currentCenter
                double distance = Vectors.sqdist(centers.get(iCenter), P.get(iPoint));

//        if the current center is closer, set it as the closest center for the point
                if (distance < dFromClosestCenter.get(iPoint)){
                    dFromClosestCenter.set(iPoint, distance);
                    closestCenter.set(iPoint, iCenter);

                    //compute the probability for the point
                    double wpdp = WP.get(iPoint) * distance;
                    pointsProb.set(iPoint, wpdp);
                }

                totalwpdp += pointsProb.get(iPoint);

            }

            //draw a random number in [0, 1)
            double rnd = Math.random();

            double cumProb = 0;
            boolean centerFound = false;

            //compute the cumulative probability and check if the point is drawn (the random number belongs to
            // the probability interval of the point)
            for (int iPoint = 0; iPoint < P.size() && !centerFound; iPoint++) {

                //normalize the probability to 1
                double pointProb = pointsProb.get(iPoint)/ totalwpdp;

                cumProb += pointProb;

                if (rnd <= cumProb) {
                    centerFound = true;
                    //add the chosen point to the list of centers
                    centers.add(P.get(iPoint));

                    //remove the point from P
                    P.remove(iPoint);
                    dFromClosestCenter.remove(iPoint);
                    closestCenter.remove(iPoint);
                    pointsProb.remove(iPoint);

                }
            }

        }

        //add to the list of points the selected centers
        for(Vector c : centers) {
            P.add(c);
        }

        //Return the set of centers
        return centers;

    }

    /*
compute the verage squared distance of a point of P from its closest center
(i.e., the kmeans objective function for P with centers C, divided by the number of points of P)
*/
    public static double kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C) {


        double totDist = 0;
        for (int iPoint = 0; iPoint < P.size(); iPoint++) {

            double pointDistance = Vectors.sqdist(P.get(iPoint), C.get(0));

//      find the closest center for the point
            for (int iCenter = 1; iCenter < C.size(); iCenter++) {

                double distCurrCenter = Vectors.sqdist(C.get(iCenter), P.get(iPoint));
                if (distCurrCenter < pointDistance) {
                    pointDistance = distCurrCenter;
                }

            }

//    compute the total distance
            totDist += pointDistance;

        }

        return totDist / P.size();

    }

    public static ArrayList<Long> computeCoresetWeights(ArrayList<Vector> P, ArrayList<Vector> C) {

        ArrayList<Long> weights = new ArrayList<>(Collections.nCopies(C.size(), 0L));

        for (int iPoint = 0; iPoint < P.size(); iPoint++) {

            double pointDistance = Vectors.sqdist(P.get(iPoint), C.get(0));

//      find the closest center for the point
            int center = 0;
            for (int iCenter = 1; iCenter < C.size(); iCenter++) {

                double distCurrCenter = Vectors.sqdist(C.get(iCenter), P.get(iPoint));
                if (distCurrCenter < pointDistance) {
                    pointDistance = distCurrCenter;
                    center = iCenter;
                }

            }
            weights.set(center, weights.get(center) + 1L);

        }

        return weights;
    }
}

