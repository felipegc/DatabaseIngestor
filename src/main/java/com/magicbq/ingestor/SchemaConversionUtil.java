package com.magicbq.ingestor;

import com.google.schemaconversion.converter.bigquery.OracleBigQueryConverter;
import com.google.schemaconversion.exceptions.SourceException;
import com.google.schemaconversion.source.SourceConnectionConfiguration;
import com.google.schemaconversion.source.oracle.OracleConnector;
import com.google.schemaconversion.source.oracle.OracleSourceDefinition;
import com.google.schemaconversion.source.schema.Database;
import com.google.schemaconversion.target.bigquery.Dataset;
import com.google.schemaconversion.target.bigquery.Table;
import com.magicbq.ingestor.Schemas.ColumnInfo;
import com.magicbq.ingestor.Schemas.TableInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class SchemaConversionUtil {

  private OracleSourceDefinition getSourceDefinition(String schemaName, OjdbcConnector connector)
      throws Exception {
    SourceConnectionConfiguration connectionConfiguration =
        new SourceConnectionConfiguration()
            .setHost(connector.getHost())
            .setPort(connector.getPort())
            .setPassword(connector.getCredentials().getPassword())
            .setUsername(connector.getCredentials().getUsername())
            .setDatabase(connector.getDatabase());
    OracleSourceDefinition sourceDefinition;
    try {
      sourceDefinition = new OracleConnector(connectionConfiguration).loadSourceDefinition();
    } catch (SourceException libraryException) {
      throw new Exception(libraryException); // TODO: felipegc improve the exception
    }

    // Need to convert only the passed one schema.
    for (Map.Entry<String, Database> entry : sourceDefinition.getDatabases().entrySet()) {
      entry.getValue().setConvert(entry.getKey().equalsIgnoreCase(schemaName));
    }
    return sourceDefinition;
  }

  private List<TableInfo> listTables(
      OracleSourceDefinition sourceDefinition, String schemaName, String[] tableNamesPattern) {
    OracleBigQueryConverter converter = new OracleBigQueryConverter();
    Properties conversionProperties = new Properties();
    Map<String, Dataset> dataSets =
        converter.convert(sourceDefinition, conversionProperties).getDatasets();

    for (Map.Entry<String, Dataset> datasetEntry : dataSets.entrySet()) {
      if (datasetEntry.getValue().getSourceObject().getName().equalsIgnoreCase(schemaName)) {
        return getDatabaseInfoList(
            datasetEntry.getValue(), datasetEntry.getKey(), tableNamesPattern);
      }
    }
    //return new Schemas(new DatabaseInfo[] {});
    return new ArrayList<>();
  }

  private List<TableInfo> getDatabaseInfoList(
      Dataset dataset, String schemaName, String[] tableNamesPattern) {
    // Note that we are comparing and matching all strings in lower case.
    String pattern =
        Arrays.stream(tableNamesPattern)
            .map(x -> String.format("(%s)", x.toLowerCase(Locale.getDefault())))
            .collect(Collectors.joining("|"));
    List<TableInfo> tableInfoList = new ArrayList<>();

    List<String> tableNames =
        dataset.getTables().entrySet().stream()
            .filter(
                entry ->
                    entry
                        .getValue()
                        .getSourceObject()
                        .getName()
                        .toLowerCase(Locale.getDefault())
                        .matches(pattern))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

    for (String tableName : tableNames) {
      Table t = dataset.getTables().get(tableName);
      tableInfoList.add(
          new TableInfo(
              tableName,
              t.getSourceObject().getName(),
              t.getColumns().values().stream()
                  .map(
                      c ->
                          new ColumnInfo(
                              c.getName(),
                              c.getSourceObject().getName(),
                              c.getType(),
                              c.getMapping().getFrom()))
                  .toArray(ColumnInfo[]::new)));
    }

    return tableInfoList;
    //    DatabaseInfo dbInfo =
    //        new DatabaseInfo(
    //            schemaName,
    //            dataset.getSourceObject().getName(),
    //            tableInfoList.toArray(new TableInfo[0]));
    //    return new Schemas(new DatabaseInfo[] {dbInfo});
  }

  /**
   * Retrieves the table info fetched on oracle by schema conversion tool.
   *
   * @param schema         what schema to be searched by.
   * @param tables         the tables we want to retrieve.
   * @param ojdbcConnector holds the whole information to connect to oracle.
   */
  public List<TableInfo> getTablesInfo(String schema, String[] tables,
      OjdbcConnector ojdbcConnector) throws Exception { // TODO: felipegc treat this exception
    OracleSourceDefinition sourceDefinition = getSourceDefinition(schema,
        ojdbcConnector); // TODO: felipegc validate if we need all these schema/database stuff
    List<TableInfo> tableInfos = listTables(sourceDefinition, schema, tables);
    // TODO: felipegc remove this
    // GsonBuilder gsonBuilder = new GsonBuilder(); // TODO: felipegc remove
    // final String opa = gsonBuilder.create().toJson(tableInfos);
    //System.out.println(opa);

    return tableInfos;
  }
}
