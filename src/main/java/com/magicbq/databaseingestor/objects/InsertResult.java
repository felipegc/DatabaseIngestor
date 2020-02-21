package com.magicbq.databaseingestor.objects;

public class InsertResult {
  private String name;
  private String timeSpent;

  public InsertResult(String name, String timestamp) {
    this.name = name;
    this.timeSpent = timestamp;
  }

  @Override
  public String toString() {
    return String.format(
        "Insert summary: [name=%s, total time spent=%s]", this.name, this.timeSpent);
  }
}
