package com.magicbq.ingestor;

import com.magicbq.ingestor.Schemas.TableInfo;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import oracle.jdbc.OracleResultSet;
import oracle.sql.BFILE;

public class App {

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new Exception("The setup.properties is missing.");
    }

    Properties prop = new Properties();
    FileInputStream ip = new FileInputStream(args[0]);
    prop.load(ip);
    String host = prop.getProperty("HOST");
    int portNumber = Integer.parseInt(prop.getProperty("PORT_NUMBER"));
    String database = prop.getProperty("DATABASE");
    String username = prop.getProperty("USER_NAME");
    String password = prop.getProperty("PASSWORD");
    String schemaToBePopulated = prop.getProperty("SCHEMA_TO_POPULATE");
    String[] tables = prop.getProperty("TABLES").split(",");
    int numberOfLinesPerThread = Integer.parseInt(prop.getProperty("NUMBER_OF_LINES_PER_THREAD"));
    int numberOfThreads = Integer.parseInt(prop.getProperty("NUMBER_OF_THREADS"));
    int batchSize = Integer.parseInt(prop.getProperty("BATCH_SIZE"));

    Credentials credentials =
        Credentials.newBuilder().withUsername(username).withPassword(password).build();

    OjdbcConnector ojdbcConnector =
        OjdbcConnector.newBuilder()
            .withHost(host)
            .withPort(portNumber)
            .withDatabase(database)
            .withCredentials(credentials)
            .withMaxSessions(1)
            .build();

    SchemaConversionUtil schemaConversionUtil = new SchemaConversionUtil();
    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

    List<TableInfo> tablesInfo =
        schemaConversionUtil.getTablesInfo(schemaToBePopulated, tables, ojdbcConnector);

    long start = System.currentTimeMillis();

    for (TableInfo tableInfo : tablesInfo) {
      List<DataGenerator> futures = new ArrayList<>();
      for (int i = 0; i < numberOfThreads; i++) {
        String executorName = tableInfo.getName() + "-" + (i + 1);
        futures.add(
            new DataGenerator(
                tableInfo,
                schemaToBePopulated,
                numberOfLinesPerThread,
                batchSize,
                ojdbcConnector,
                executorName));
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

  /**
   * BFILE is a pointer to a file which resides on database server. As an example imagine a dir on
   * /home/work' dir and it contains a file called 'file1.data'. In order to do this test we should
   * create the /home/work/ dir on oracle server. Then we give permission to this folder and then we
   * create a file called file1.data.
   *
   * <pre>
   * Later we should run the following oracle commands:
   *
   * #Create a dir:
   * CREATE OR REPLACE DIRECTORY test_dir AS '/home/work';
   *
   * #Create a table with file type:
   * CREATE TABLE {SCHEMA}.{TABLE} (x varchar2 (30), b bfile);
   *
   * #Insert a row for this example:
   * INSERT INTO FELIPE.TEST VALUES ('one', BFILENAME('TEST_DIR', 'file1.data'));
   *
   * obs: we can't upload file remotely. It must be on server even though we can read it from it.
   * The following script shows this.
   * </pre>
   */
  static void readBFileContent(String schema, OjdbcConnector ojdbcConnector) {
    System.out.println("Reading BFile content...");
    try (Connection conn = ojdbcConnector.getConnection()) {
      Statement statement = conn.createStatement();
      String cmd = "SELECT *  FROM FELIPE.TEST WHERE x = 'one'";
      ResultSet rs = statement.executeQuery(cmd);

      if (rs.next()) {
        System.out.println("FOUND IT");
        BFILE bfile = ((OracleResultSet) rs).getBFILE(2);

        // for these methods, you do not have to open the bfile
        System.out.println("getDirAlias() = " + bfile.getDirAlias());
        System.out.println("getName() = " + bfile.getName());
        System.out.println("fileExists() = " + bfile.fileExists());
        System.out.println("isFileOpen() = " + bfile.isFileOpen());

        // now open the bfile to get the data
        bfile.openFile();

        // get the BFILE data as a binary stream
        InputStream in = bfile.getBinaryStream();
        int length;

        // read the bfile data in 6-byte chunks
        byte[] buf = new byte[6];

        while ((length = in.read(buf)) != -1) {
          // append and display the bfile data in 6-byte chunks
          StringBuffer sb = new StringBuffer(length);
          for (int i = 0; i < length; i++) {
            sb.append((char) buf[i]);
          }
          System.out.println(sb.toString());
        }

        // we are done working with the input stream. Close it.
        in.close();

        // we are done working with the BFILE. Close it.
        bfile.closeFile();
      }
    } catch (Exception ex) {
      System.out.println(ex);
    }
  }
}
