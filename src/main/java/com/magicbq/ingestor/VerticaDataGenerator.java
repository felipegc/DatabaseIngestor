package com.magicbq.ingestor;

import com.magicbq.ingestor.Schemas.ColumnInfo;
import com.magicbq.ingestor.Schemas.TableInfo;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class VerticaDataGenerator extends DataGenerator {

  public VerticaDataGenerator(
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
    return Arrays.stream(tableInfo.getColumns()).collect(Collectors.toList());
  }
}
