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
      String[] files = {"vecs-50-10000.txt","vecs-50-50000.txt","vecs-50-100000.txt","vecs-50-500000.txt"};
      Boolean check = false;
      Scanner keyboard = new Scanner(System.in);
      int n = -1;

      while(check==false)
      {
          System.out.println("Choose the dataset: \n [0] 9960 points \n [1] 50047 points \n [2] 99670 points \n [3] 499950 points");
          n= keyboard.nextInt();
          if(n<4 && n>-1)
              check=true;
          else
              System.out.println("The typed number is incorrect");
      }

      ArrayList<Vector> P = InputOutput.readVectorsSeq(files[n]);
      

      System.out.println("Choose the number of k centers");
      int k= keyboard.nextInt();
      System.out.println("Choose the number of k1 centers");
      int k1= keyboard.nextInt();

      //run kcenter and print the running time
      System.out.println("**************** Running kcenter ****************");
      long startKCenters = System.currentTimeMillis();

      long endKCenters = System.currentTimeMillis();
      System.out.println("Time elapsed for kcenters: " + (endKCenters - startKCenters));



      //run kmeansPP

      ArrayList<Long> weights = new ArrayList<>(Collections.nCopies(P.size(), 1L));
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



//      long startkc = System.currentTimeMillis();
//      ArrayList<Vector> C = kcenter(P,k);
//      long endkc = System.currentTimeMillis();
//
//      System.out.println("********* CENTERS ********");
//
//      for(Vector vector : C)
//          System.out.println(vector);
//
//      System.out.println("\n********* TIME PERFORMANCES ********");
//      System.out.println("Elapsed time with kcenter: " + (endkc - startkc) + " ms");
//
//      System.out.println("Press Enter to terminate: ");
//
//      try {
//          System.in.read();
//      } catch (Exception e) {
//
//      }


  }

    private static ArrayList<Vector> kcenter(ArrayList<Vector> P, int k){

    ArrayList <Vector> centers= new ArrayList<>();
    //Assign the first element of the ArrayList as first arbitrary center c1 and remove it from the list of points;
    centers.add(P.get(0));
    P.remove(0);
    //Define an ArrayList of double which will contains the minimum distance of a point from the set S (set of centers)
    ArrayList< Double > distances = new ArrayList<>(P.size());
    //Start the Farthest-first traversal algorithm
    for (int i = 0; i < k-1; i++) {
      //Define a neutral maximum distance from the set S and a neutral index
      int indexMaxPoint = -1;
      double max = -1;
      //Iterate among all the points of the dataset
      for (int j = 0; j < P.size(); j++) {
        //Compute the distance between the point j and the center i
        double distance= Vectors.sqdist(centers.get(i), P.get(j));
        //If it's the first iteration (i=0), add the distance to the ArrayList distances,
        //If it's not the first iteration(i!=0) and the distance is lower than the one on the ArrayList, update it
          if(i==0)
            distances.add(j,Vectors.sqdist(centers.get(i), P.get(j)));
          if(i!=0 && distances.get(j) > distance)
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

