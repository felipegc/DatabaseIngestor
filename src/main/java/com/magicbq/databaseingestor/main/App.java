package com.magicbq.databaseingestor.main;

import com.magicbq.databaseingestor.ingestor.IngestorService;
import com.magicbq.databaseingestor.ingestor.OracleIngestorService;
import com.magicbq.databaseingestor.ingestor.VerticaIngestorService;
import com.magicbq.databaseingestor.objects.Credentials;
import com.magicbq.databaseingestor.objects.OjdbcConnector;
import com.magicbq.databaseingestor.schemaconversion.OracleSchemaConversion;
import com.magicbq.databaseingestor.schemaconversion.SchemaConversion;
import com.magicbq.databaseingestor.schemaconversion.VerticaSchemaConversion;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import oracle.jdbc.OracleResultSet;
import oracle.sql.BFILE;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class App {

  /** Main method which starts the program. */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new Exception("The setup.properties is missing.");
    }

    getIngestor(args[0]).ingestDatabase();
  }

  static IngestorService getIngestor(String fileArg) throws NotImplementedException, IOException {

    Properties prop = new Properties();
    FileInputStream ip = new FileInputStream(fileArg);
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
    String sourceType = prop.getProperty("SOURCE_TYPE");

    Credentials credentials =
        Credentials.newBuilder().withUsername(username).withPassword(password).build();

    OjdbcConnector ojdbcConnector;
    SchemaConversion schemaConversion;

    switch (sourceType.toUpperCase()) {
      case "ORACLE":
        ojdbcConnector =
            OjdbcConnector.newBuilder()
                .withConnectionString(OracleIngestorService.JDBC_CONNECTION_STRING)
                .withDriverString(OracleIngestorService.DRIVER_STRING)
                .withHost(host)
                .withPort(portNumber)
                .withDatabase(database)
                .withCredentials(credentials)
                .withMaxSessions(1)
                .build();
        schemaConversion = new OracleSchemaConversion(ojdbcConnector);

        return new OracleIngestorService(
            ojdbcConnector,
            schemaConversion,
            numberOfThreads,
            schemaToBePopulated,
            tables,
            numberOfLinesPerThread,
            batchSize);
      case "VERTICA":
        ojdbcConnector =
            OjdbcConnector.newBuilder()
                .withConnectionString(VerticaIngestorService.JDBC_CONNECTION_STRING)
                .withDriverString(VerticaIngestorService.DRIVER_STRING)
                .withHost(host)
                .withPort(portNumber)
                .withDatabase(database)
                .withCredentials(credentials)
                .withMaxSessions(1)
                .build();
        schemaConversion = new VerticaSchemaConversion(ojdbcConnector);

        return new VerticaIngestorService(
            ojdbcConnector,
            schemaConversion,
            numberOfThreads,
            schemaToBePopulated,
            tables,
            numberOfLinesPerThread,
            batchSize);
      default:
        System.out.println("This source is not implemented yet");
        throw new NotImplementedException();
    }
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
