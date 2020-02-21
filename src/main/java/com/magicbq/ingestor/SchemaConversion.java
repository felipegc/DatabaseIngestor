package com.magicbq.ingestor;

import com.google.schemaconversion.converter.bigquery.RDBMSBigQueryConverter;
import com.google.schemaconversion.exceptions.SourceException;
import com.google.schemaconversion.source.SourceConnectionConfiguration;
import com.google.schemaconversion.source.rdbms.RdbmSourceDefinition;
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

public abstract class SchemaConversion {

  OjdbcConnector ojdbcConnector;

  public SchemaConversion(OjdbcConnector ojdbcConnector) {
    this.ojdbcConnector = ojdbcConnector;
  }

  public abstract RdbmSourceDefinition loadSourceDefinition(
      SourceConnectionConfiguration connectionConfiguration) throws Exception;

  public abstract RDBMSBigQueryConverter getBigQueryConverter();

  public RdbmSourceDefinition getSourceDefinition(String schemaName) throws Exception {

    SourceConnectionConfiguration connectionConfiguration =
        new SourceConnectionConfiguration()
            .setHost(ojdbcConnector.getHost())
            .setPort(ojdbcConnector.getPort())
            .setPassword(ojdbcConnector.getCredentials().getPassword())
            .setUsername(ojdbcConnector.getCredentials().getUsername())
            .setDatabase(ojdbcConnector.getDatabase());

    RdbmSourceDefinition sourceDefinition;

    try {
      sourceDefinition = loadSourceDefinition(connectionConfiguration);
    } catch (SourceException libraryException) {
      throw new Exception(libraryException);
    }

    // Need to convert only the passed one schema.
    for (Map.Entry<String, Database> entry : sourceDefinition.getDatabases().entrySet()) {
      entry.getValue().setConvert(entry.getKey().equalsIgnoreCase(schemaName));
    }
    return sourceDefinition;
  }

  private List<TableInfo> getDatabaseInfoList(Dataset dataset, String[] tableNamesPattern) {
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
  }

  private List<TableInfo> listTables(
      RdbmSourceDefinition sourceDefinition, String schemaName, String[] tableNamesPattern) {

    RDBMSBigQueryConverter converter = getBigQueryConverter();

    Properties conversionProperties = new Properties();
    Map<String, Dataset> dataSets =
        converter.convert(sourceDefinition, conversionProperties).getDatasets();

    for (Map.Entry<String, Dataset> datasetEntry : dataSets.entrySet()) {
      if (datasetEntry.getValue().getSourceObject().getName().equalsIgnoreCase(schemaName)) {
        return getDatabaseInfoList(datasetEntry.getValue(), tableNamesPattern);
      }
    }
    return new ArrayList<>();
  }

  /**
   * Retrieves the table info fetched on database by schema conversion tool.
   *
   * @param schema what schema to be searched by.
   * @param tables the tables we want to retrieve.
   */
  public List<TableInfo> getTablesInfo(String schema, String[] tables)
      throws Exception { // TODO: felipegc treat this exception

    RdbmSourceDefinition sourceDefinition = getSourceDefinition(schema);

    List<TableInfo> tableInfos = listTables(sourceDefinition, schema, tables);

    return tableInfos;
  }
}
