package com.magicbq.ingestor;

import com.magicbq.ingestor.Schemas.ColumnInfo;
import com.magicbq.ingestor.Schemas.TableInfo;
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
