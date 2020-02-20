package com.magicbq.ingestor;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import oracle.sql.ANYDATA;
import oracle.sql.NUMBER;

public class TypeGeneratorUtil {

  private static final Map<String, ColumnData> TYPE_GENERATOR =
      new HashMap<>();

  private static final ColumnData NUMBER_GENERATOR =
      (PreparedStatement ps, int index) -> {
        int fakeInt = Integer.MAX_VALUE;
        ps.setInt(index, fakeInt);
      };

  private static final ColumnData LONG_GENERATOR =
      (PreparedStatement ps, int index) -> {
        long fakeLong = Long.MAX_VALUE;
        ps.setLong(index, fakeLong);
      };

  private static final ColumnData NCHAR_GENERATOR =
      (PreparedStatement ps, int index) -> {
        String fakeNChar = "N";
        ps.setString(index, fakeNChar);
      };

  private static final ColumnData UROWID_GENERATOR =
      (PreparedStatement ps, int index) -> {
        String fakeUrowid = "AAAMaHAAEAAAAIHAAZ";
        ps.setString(index, fakeUrowid);
      };

  private static final ColumnData VARCHAR2_GENERATOR =
      (PreparedStatement ps, int index) -> {
        String fakeVarchar2 = "PAKISTAN";//this is a fake varchar2
        ps.setString(index, fakeVarchar2);
      };

  private static final ColumnData NVARCHAR2_GENERATOR =
      (PreparedStatement ps, int index) -> {
        String fakeNvarchar2 = "this is a fake nvarchar2";
        ps.setString(index, fakeNvarchar2);
      };

  //  private static final ColumnData ROWID_GENERATOR =
  //      (PreparedStatement ps, int index) -> {
  //        String fakeRowid = "this is a rowid";
  //        ps.setRowId(index, fakeRowid);
  //      };

  private static final ColumnData CHAR_GENERATOR =
      (PreparedStatement ps, int index) -> {
        String fakeChar = "C";
        ps.setString(index, fakeChar);
      };

  private static final ColumnData CLOB_GENERATOR =
      (PreparedStatement ps, int index) -> {
        String fakeClob = "this is a clob much bigger then varchar";
        ps.setString(index, fakeClob);
      };

  private static final ColumnData NCLOB_GENERATOR =
      (PreparedStatement ps, int index) -> {
        String fakeNClob = "This is Nclob";
        ps.setString(index, fakeNClob);
      };

  //  private static final ColumnData BFILE_GENERATOR =
  //      (PreparedStatement ps, int index) -> {
  //
  //      };

  private static final ColumnData BLOB_GENERATOR =
      (PreparedStatement ps, int index) -> {
        byte[] bytes = "This is a sample BLOB huahauhau".getBytes();
        ps.setBytes(index, bytes);
      };

  private static final ColumnData LONG_RAW_GENERATOR =
      (PreparedStatement ps, int index) -> {
        byte[] bytes = "This is a sample of LONG RAW....".getBytes();
        ps.setBytes(index, bytes);
      };

  private static final ColumnData RAW_GENERATOR =
      (PreparedStatement ps, int index) -> {
        byte[] bytes = "This is a sample of RAW....".getBytes();
        ps.setBytes(index, bytes);
      };

  private static final ColumnData INTERVAL_YEAR_GENERATOR =
      (PreparedStatement ps, int index) -> {
        String intervalYear = "10-3"; // 10 years and 3 months
        ps.setString(index, intervalYear);
      };

  private static final ColumnData BINARY_DOUBLE_GENERATOR =
      (PreparedStatement ps, int index) -> {
        double binaryDouble = 3687423874623874364873.7643746;
        ps.setDouble(index, binaryDouble);
      };

  private static final ColumnData BINARY_FLOAT_GENERATOR =
      (PreparedStatement ps, int index) -> {
        float binaryFloat = 12.7F;
        ps.setDouble(index, binaryFloat);
      };

  private static final ColumnData DATE_GENERATOR =
      (PreparedStatement ps, int index) -> {
        long timestamp = System.currentTimeMillis();
        Date date = new Date(timestamp);
        ps.setDate(index, date);
      };

  private static final ColumnData TIMESTAMP_GENERATOR =
      (PreparedStatement ps, int index) -> {
        long timestamp = System.currentTimeMillis();
        Date date = new Date(timestamp);
        ps.setDate(index, date);
      };

  private static final ColumnData ANYDATA_GENERATOR =
      (PreparedStatement ps, int index) -> {
        //String anyData = "anydata is the best type ever";
        NUMBER number = new NUMBER(12345);
        ANYDATA anyData = ANYDATA.convertDatum(number);
        ps.setObject(index, anyData);
      };

  private static final ColumnData FLOAT_GENERATOR =
      (PreparedStatement ps, int index) -> {
        float floatData = 7.8F;
        ps.setFloat(index, floatData);
      };

  private static final ColumnData INTERVAL_DAY_GENERATOR =
      (PreparedStatement ps, int index) -> {
        String interval = "3 2:25:0.0";
        ps.setString(index, interval);
      };

  //  private static final ColumnData SDO_GEOMETRY_GENERATOR =
  //      (PreparedStatement ps, int index) -> {
  //        try {
  //          double[] cord = [3.0, 3.0];
  //          JGeometry j_geom = JGeometry.createPoint(cord, 3, 3);
  //          STRUCT str = JGeometry.store();
  //          ps.setObject(index, str);
  //        } catch (Exception ex) {
  //
  //        }
  //      };

  private static final ColumnData XMLTYPE_GENERATOR =
      (PreparedStatement ps, int index) -> {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "            <list_book>\n"
            + "                <book>1</book>\n"
            + "                <book>2</book>\n"
            + "                <book>3</book>\n"
            + "            </list_book>";
        ps.setString(index, xml);
      };

  private static final ColumnData TYPE_TIMESTAMP_WITH_LOCAL_TZ =
      (PreparedStatement ps, int index) -> {
        long timestamp = System.currentTimeMillis();
        Date date = new Date(timestamp);
        ps.setDate(index, date);
      };

  private static final ColumnData TYPE_TIMESTAMP_WITH_TZ =
      (PreparedStatement ps, int index) -> {
        long timestamp = System.currentTimeMillis();
        Date date = new Date(timestamp);
        ps.setDate(index, date);
      };

  static {
    TYPE_GENERATOR.put("long", LONG_GENERATOR);
    TYPE_GENERATOR.put("nchar", NCHAR_GENERATOR);
    TYPE_GENERATOR.put("urowid", UROWID_GENERATOR);
    TYPE_GENERATOR.put("varchar2", VARCHAR2_GENERATOR);
    TYPE_GENERATOR.put("nvarchar2", NVARCHAR2_GENERATOR);
    //TYPE_GENERATOR.put("rowid", ROWID_GENERATOR);// seems we don't need to worry about
    TYPE_GENERATOR.put("char", CHAR_GENERATOR);
    TYPE_GENERATOR.put("clob", CLOB_GENERATOR);
    TYPE_GENERATOR.put("nclob", NCLOB_GENERATOR);
    //TYPE_GENERATOR.put("bfile", BFILE_GENERATOR);
    TYPE_GENERATOR.put("blob", BLOB_GENERATOR);
    TYPE_GENERATOR.put("long raw", LONG_RAW_GENERATOR);
    TYPE_GENERATOR.put("raw", RAW_GENERATOR);
    TYPE_GENERATOR.put("interval year", INTERVAL_YEAR_GENERATOR);
    TYPE_GENERATOR.put("number", NUMBER_GENERATOR);
    TYPE_GENERATOR.put("binary_double", BINARY_DOUBLE_GENERATOR);
    TYPE_GENERATOR.put("binary_float", BINARY_FLOAT_GENERATOR);
    TYPE_GENERATOR.put("date", DATE_GENERATOR);
    TYPE_GENERATOR.put("timestamp", TIMESTAMP_GENERATOR);
    TYPE_GENERATOR.put("anydata", ANYDATA_GENERATOR);
    TYPE_GENERATOR.put("float", FLOAT_GENERATOR);
    TYPE_GENERATOR.put("interval day", INTERVAL_DAY_GENERATOR);
    //TYPE_GENERATOR.put("sdo geometry", SDO_GEOMETRY_GENERATOR);
    TYPE_GENERATOR.put("xmltype", XMLTYPE_GENERATOR);
    TYPE_GENERATOR.put("timestamp with local time zone",TYPE_TIMESTAMP_WITH_LOCAL_TZ);
    TYPE_GENERATOR.put("timestamp with time zone",TYPE_TIMESTAMP_WITH_TZ);

  }

  public static void generateValue(String columnType, PreparedStatement ps, int index)
      throws SQLException {
    TypeGeneratorUtil.TYPE_GENERATOR.get(columnType).generate(ps, index);
  }

  @FunctionalInterface
  protected interface ColumnData {
    void generate(PreparedStatement ps, int index) throws SQLException;
  }
}
