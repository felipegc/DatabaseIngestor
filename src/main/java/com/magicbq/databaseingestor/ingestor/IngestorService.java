package com.magicbq.databaseingestor.ingestor;

import com.magicbq.databaseingestor.datagenerator.DataGenerator;
import com.magicbq.databaseingestor.objects.InsertResult;
import com.magicbq.databaseingestor.objects.OjdbcConnector;
import com.magicbq.databaseingestor.objects.Schemas.TableInfo;
import com.magicbq.databaseingestor.schemaconversion.SchemaConversion;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class IngestorService {

  ExecutorService executor;

  OjdbcConnector ojdbcConnector;
  SchemaConversion schemaConversion;
  int numberOfThreads;
  String schemaToBePopulated;
  String[] tables;
  int numberOfLinesPerThread;
  int batchSize;

  /** Constructor for IngestorService. */
  public IngestorService(
      OjdbcConnector ojdbcConnector,
      SchemaConversion schemaConversion,
      int numberOfThreads,
      String schemaToBePopulated,
      String[] tables,
      int numberOfLinesPerThread,
      int batchSize) {
    this.ojdbcConnector = ojdbcConnector;
    this.schemaConversion = schemaConversion;
    this.numberOfThreads = numberOfThreads;
    this.schemaToBePopulated = schemaToBePopulated;
    this.tables = tables;
    this.numberOfLinesPerThread = numberOfLinesPerThread;
    this.batchSize = batchSize;

    executor = Executors.newFixedThreadPool(numberOfThreads);
  }

  /** Gets the object instance for the DataGenerator source. */
  public abstract DataGenerator getDataGenerator(TableInfo tableInfo, String executorName);

  /**
   * Handles the database ingestion. It gets the table info and distributes the ingestion between
   * the treads.
   */
  public void ingestDatabase() throws Exception {

    long start = System.currentTimeMillis();

    List<TableInfo> tablesInfo = this.schemaConversion.getTablesInfo(schemaToBePopulated, tables);

    for (TableInfo tableInfo : tablesInfo) {
      List<DataGenerator> futures = new ArrayList<>();
      for (int i = 0; i < numberOfThreads; i++) {
        String executorName = tableInfo.getName() + "-" + (i + 1);
        futures.add(getDataGenerator(tableInfo, executorName));
      }

      List<Future<InsertResult>> resultList = null;

      try {
        resultList = executor.invokeAll(futures);
      } catch (Exception e) {
        System.out.println("Error while invoking all threads");
        e.printStackTrace();
      }

      System.out.println("Summary results");

      for (int i = 0; i < futures.size(); i++) {
        Future<InsertResult> future = resultList.get(i);
        try {
          InsertResult result = future.get();
          System.out.println(result.toString());
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
    }

    executor.shutdown();

    long end = System.currentTimeMillis();
    long totalTimeInsert = (end - start) / 1000;
    System.out.println("Total Time for the whole operation: " + totalTimeInsert);
  }
}
