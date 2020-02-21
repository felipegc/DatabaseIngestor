package com.magicbq.databaseingestor.ingestor;

import com.magicbq.databaseingestor.datagenerator.DataGenerator;
import com.magicbq.databaseingestor.datagenerator.OracleDataGenerator;
import com.magicbq.databaseingestor.objects.OjdbcConnector;
import com.magicbq.databaseingestor.objects.Schemas.TableInfo;
import com.magicbq.databaseingestor.schemaconversion.SchemaConversion;

public class OracleIngestorService extends IngestorService {

  public static final String JDBC_CONNECTION_STRING = "jdbc:oracle:thin:@%s:%s:%s";
  public static final String DRIVER_STRING = "oracle.jdbc.driver.OracleDriver";

  /**
   * Constructor for Oracle Ingestor Service.
   */
  public OracleIngestorService(
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
    return new OracleDataGenerator(
        tableInfo,
        schemaToBePopulated,
        numberOfLinesPerThread,
        batchSize,
        ojdbcConnector,
        executorName);
  }
}
