package com.magicbq.databaseingestor.schemaconversion;

import com.google.schemaconversion.converter.bigquery.OracleBigQueryConverter;
import com.google.schemaconversion.source.SourceConnectionConfiguration;
import com.google.schemaconversion.source.oracle.OracleConnector;
import com.google.schemaconversion.source.oracle.OracleSourceDefinition;
import com.magicbq.databaseingestor.objects.OjdbcConnector;

public class OracleSchemaConversion extends SchemaConversion {

  public OracleSchemaConversion(OjdbcConnector ojdbcConnector) {
    super(ojdbcConnector);
  }

  @Override
  public OracleSourceDefinition loadSourceDefinition(
      SourceConnectionConfiguration connectionConfiguration) throws Exception {
    return new OracleConnector(connectionConfiguration).loadSourceDefinition();
  }

  @Override
  public OracleBigQueryConverter getBigQueryConverter() {
    return new OracleBigQueryConverter();
  }
}
