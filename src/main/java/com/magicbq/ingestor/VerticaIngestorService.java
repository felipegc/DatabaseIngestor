package com.magicbq.ingestor;

import com.magicbq.ingestor.Schemas.TableInfo;

public class VerticaIngestorService extends IngestorService {

  public static final String JDBC_CONNECTION_STRING = "jdbc:vertica://%s:%s/%s";
  public static final String DRIVER_STRING = "com.vertica.jdbc.Driver";

  public VerticaIngestorService(
      OjdbcConnector ojdbcConnector,
      SchemaConversion schemaConversion,
      int numberOfThreads,
      String schemaToBePopulated,
      String[] tables,
      int numberOfLinesPerThread,
      int batchSize) {
    super(
        ojdbcConnector,
        schemaConversion,
        numberOfThreads,
        schemaToBePopulated,
        tables,
        numberOfLinesPerThread,
        batchSize);
  }

  @Override
  public DataGenerator getDataGenerator(TableInfo tableInfo, String executorName) {
    return new VerticaDataGenerator(
        tableInfo,
        schemaToBePopulated,
        numberOfLinesPerThread,
        batchSize,
        ojdbcConnector,
        executorName);
  }
}
