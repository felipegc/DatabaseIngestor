package com.magicbq.databaseingestor.schemaconversion;

import com.google.schemaconversion.converter.bigquery.VerticaBigQueryConverter;
import com.google.schemaconversion.source.SourceConnectionConfiguration;
import com.google.schemaconversion.source.vertica.VerticaConnector;
import com.google.schemaconversion.source.vertica.VerticaSourceDefinition;
import com.magicbq.databaseingestor.objects.OjdbcConnector;

public class VerticaSchemaConversion extends SchemaConversion {

  public VerticaSchemaConversion(OjdbcConnector ojdbcConnector) {
    super(ojdbcConnector);
  }

  @Override
  public VerticaSourceDefinition loadSourceDefinition(
      SourceConnectionConfiguration connectionConfiguration) throws Exception {
    return new VerticaConnector(connectionConfiguration).loadSourceDefinition();
  }

  @Override
  public VerticaBigQueryConverter getBigQueryConverter() {
    return new VerticaBigQueryConverter();
  }
}
