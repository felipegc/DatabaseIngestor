package com.magicbq.databaseingestor.ingestor;

import com.magicbq.databaseingestor.datagenerator.DataGenerator;
import com.magicbq.databaseingestor.datagenerator.VerticaDataGenerator;
import com.magicbq.databaseingestor.objects.OjdbcConnector;
import com.magicbq.databaseingestor.objects.Schemas.TableInfo;
import com.magicbq.databaseingestor.schemaconversion.SchemaConversion;

public class VerticaIngestorService extends IngestorService {

  public static final String JDBC_CONNECTION_STRING = "jdbc:vertica://%s:%s/%s";
  public static final String DRIVER_STRING = "com.vertica.jdbc.Driver";

  /**
   * Constructor for Vertica Ingestor Service.
   */
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
