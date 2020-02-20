// Copyright 2018 Google Inc. All Rights Reserved.

package com.magicbq.ingestor;

import java.util.List;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Object representation of converted table schemas. These messages should replicate schema.proto on
 * BQ DTS side.
 */
public class Schemas {

  private DatabaseInfo[] databases;

  public Schemas(List<DatabaseInfo> databaseList) {
    this(databaseList.toArray(new DatabaseInfo[0]));
  }

  public Schemas(DatabaseInfo[] databases) {
    this.databases = ArrayUtils.clone(databases);
  }

  public DatabaseInfo[] getDatabases() {
    return ArrayUtils.clone(databases);
  }

  // TODO: felipegc remove this class.
  public static class DatabaseInfo {

    private String name;
    private String originalName;
    private TableInfo[] tables;

    public DatabaseInfo(String name, String originalName, List<TableInfo> tableList) {
      this(name, originalName, tableList.toArray(new TableInfo[0]));
    }

    /**
     * Contains the information related to database/schema(oracle).
     */
    public DatabaseInfo(String name, String originalName, TableInfo[] tables) {
      this.name = name;
      this.originalName = originalName;
      this.tables = ArrayUtils.clone(tables);
    }

    public String getOriginalName() {
      return originalName;
    }

    public TableInfo[] getTables() {
      return tables;
    }

    public String getName() {
      return name;
    }
  }


  public enum ColumnUsageType {
    DEFAULT,
    // Column used as primary key
    PRIMARY,
    // Column used as partition column in BQ
    PARTITIONING,
    // Column used as cluster column in BQ
    CLUSTERING,
    // Column used as run time tracking column in BQ DTS
    COMMIT_TIMESTAMP,
  }


  public static class ColumnInfo {

    private String name;
    private String originalName;
    private String type;
    private String originalType;
    private ColumnUsageType[] usageType;

    public ColumnInfo(String name, String originalName, String type, String originalType) {
      this(name, originalName, type, originalType, new ColumnUsageType[] {ColumnUsageType.DEFAULT});
    }

    /**
     * Contains the whole information for column.
     */
    public ColumnInfo(
        String name,
        String originalName,
        String type,
        String originalType,
        ColumnUsageType[] usageType) {
      this.name = name;
      this.originalName = originalName;
      this.type = type;
      this.originalType = originalType;
      if (usageType == null) {
        this.usageType = new ColumnUsageType[] {ColumnUsageType.DEFAULT};
      } else {
        this.usageType = ArrayUtils.clone(usageType);
      }
    }

    public String getType() {
      return type;
    }

    public String getOriginalName() {
      return originalName;
    }

    public String getName() {
      return name;
    }

    public ColumnUsageType[] getColumnUsageType() {
      return this.usageType;
    }

    public String getOriginalType() {
      return originalType;
    }
  }


  public static class TableInfo {

    private String name;
    private String originalName;
    private ColumnInfo[] columns;

    public TableInfo(String name, String originalName, List<ColumnInfo> columnList) {
      this(name, originalName, columnList.toArray(new ColumnInfo[0]));
    }

    /**
     * Contains the info for table containing its name and columns.
     */
    public TableInfo(String name, String originalName, ColumnInfo[] columns) {
      this.name = name;
      this.originalName = originalName;
      this.columns = ArrayUtils.clone(columns);
    }

    public String getOriginalName() {
      return originalName;
    }

    public ColumnInfo[] getColumns() {
      return columns;
    }

    public String getName() {
      return name;
    }
  }
}
