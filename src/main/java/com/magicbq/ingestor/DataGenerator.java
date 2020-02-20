package com.magicbq.ingestor;

import com.magicbq.ingestor.Schemas.ColumnInfo;
import com.magicbq.ingestor.Schemas.TableInfo;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Responsible for holding the thread task called by the executor. It will populate the amount of
 * lines into a table in a schema(oracle).
 */
public class DataGenerator implements Callable<InsertResult> {

  private static final String DEFAULT_INSERT_QUERY = "INSERT INTO %s.%s(%s) VALUES(%s)";

  private TableInfo tableInfo;
  private String schema;
  private Integer numberOfLinesPerThread;
  private Integer batchSize;
  private OjdbcConnector ojdbcConnector;
  private String name;

  /**
   * Public constructor.
   *
   * @param tableInfo contains the info related to table
   * @param schema schema where the tables resides in
   * @param numberOfLinesPerThread number of lines to be executed
   * @param ojdbcConnector jdbc connection info
   */
  public DataGenerator(
      TableInfo tableInfo,
      String schema,
      Integer numberOfLinesPerThread,
      Integer batchSize, // TODO: felipegc change to int
      OjdbcConnector ojdbcConnector,
      String name) {
    this.tableInfo = tableInfo;
    this.schema = schema;
    this.numberOfLinesPerThread = numberOfLinesPerThread;
    this.batchSize = batchSize;
    this.ojdbcConnector = ojdbcConnector;
    this.name = name;
  }

  @Override
  public InsertResult call() throws Exception {

    long stInsert = System.currentTimeMillis();

    List<ColumnInfo> columns =
        Arrays.stream(tableInfo.getColumns())
            .filter(
                (c) ->
                    !c.getOriginalType().equals("bfile")
                        && !c.getOriginalType().equals("sdo_geometry")
                        && !c.getOriginalType().equals("rowid"))
            .collect(Collectors.toList());

    String columnsName = columns.stream().map((c) -> c.getName()).collect(Collectors.joining(","));

    String columnsRoom = String.join(",", Collections.nCopies(columns.size(), "?"));
    String query =
        String.format(DEFAULT_INSERT_QUERY, schema, tableInfo.getName(), columnsName, columnsRoom);
    System.out.println("Executing query: " + query);

    try (Connection conn = ojdbcConnector.getConnection()) {
      int numberOfBatches = numberOfLinesPerThread / batchSize;
      int modBatch = numberOfLinesPerThread % batchSize;

      for (int a = 0; a < numberOfBatches; a++) {
        executeBatch(columns, query, conn, batchSize);
      }

      if (modBatch > 0) {
        executeBatch(columns, query, conn, modBatch);
      }
    }

    // Thread.sleep(20000);

    long enInsert = System.currentTimeMillis();
    long totalTimeInsert = (enInsert - stInsert) / 1000;
    //    System.out.println(
    //        String.format(
    //            "Time spent for inserting %s lines in %s:%s was: %s seconds",
    //            numberOfLinesPerThread, schema, tableInfo.getName(), totalTimeInsert));

    return new InsertResult(this.name, Long.toString(totalTimeInsert));
  }

  private void executeBatch(List<ColumnInfo> columns, String query, Connection conn, int rows)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(query)) {
      for (int x = 1; x <= rows; x++) {
        for (int i = 1; i <= columns.size(); i++) {
          TypeGeneratorUtil.generateValue(columns.get(i - 1).getOriginalType(), stmt, i);
        }
        stmt.addBatch();
      }

      stmt.executeBatch();
      System.out.println("Batch successfully executed");
    }
  }
}
