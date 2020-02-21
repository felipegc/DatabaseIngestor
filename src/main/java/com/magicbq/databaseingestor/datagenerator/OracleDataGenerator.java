package com.magicbq.databaseingestor.datagenerator;

import com.magicbq.databaseingestor.objects.OjdbcConnector;
import com.magicbq.databaseingestor.objects.Schemas.ColumnInfo;
import com.magicbq.databaseingestor.objects.Schemas.TableInfo;
import com.magicbq.databaseingestor.datagenerator.DataGenerator;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class OracleDataGenerator extends DataGenerator {

  public OracleDataGenerator(
      TableInfo tableInfo,
      String schema,
      Integer numberOfLinesPerThread,
      int batchSize,
      OjdbcConnector ojdbcConnector,
      String name) {
    super(tableInfo, schema, numberOfLinesPerThread, batchSize, ojdbcConnector, name);
  }

  @Override
  public List<ColumnInfo> transformAndFilterColumnInfo() {
    return Arrays.stream(tableInfo.getColumns())
        .filter(
            (c) ->
                !c.getOriginalType().equals("bfile")
                    && !c.getOriginalType().equals("sdo_geometry")
                    && !c.getOriginalType().equals("rowid"))
        .collect(Collectors.toList());
  }
}
