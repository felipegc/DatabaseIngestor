package com.magicbq.ingestor;

public class SimpleInterest {

  /**
   * SimpleInterest.
   */
  public static double calculateSimpleInterest(double amount,
      double years,
      double rateOfInterest) {
    double simpleInterest;
    simpleInterest = amount * years * rateOfInterest;
    return simpleInterest;
  }
}
