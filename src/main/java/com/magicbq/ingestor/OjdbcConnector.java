package com.magicbq.ingestor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class OjdbcConnector {

  //private static final String JDBC_CONNECTION_STRING = "jdbc:oracle:thin:@%s:%s:%s";
  private static final String JDBC_CONNECTION_STRING = "jdbc:vertica://%s:%s/%s"; // TODO:felipegc make it generic //jdbc:vertica://34.66.206.172:5433/VMart

  private Properties connectionProperties;
  private String host;
  private Integer port;
  private String database;
  private Integer maxSessions;
  private Credentials credentials;

  private OjdbcConnector(String host, Integer port, String database, Integer maxSessions,
      Credentials credentials) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.maxSessions = maxSessions;
    this.credentials = credentials;
  }

  /**
   * Build for OjdbcConnector.
   */
  public static Builder newBuilder() {
    return new OjdbcConnector.Builder();
  }

  private Properties getConnectionProperties() {
    if (connectionProperties == null) {
      connectionProperties = new Properties();
      connectionProperties.put("user", credentials.getUsername());
      connectionProperties.put("password", credentials.getPassword());
    }
    return connectionProperties;
  }

  /**
   * Get the connection to oracle.
   */
  //TODO: felipegc we should create a pool of connections. There are plenty of libs to do that.
  public Connection getConnection() throws ClassNotFoundException, SQLException {
    try {
      //Class.forName("oracle.jdbc.driver.OracleDriver"); // TODO:felipegc make it generic
      Class.forName("com.vertica.jdbc.Driver");
      return DriverManager.getConnection(
          String.format(JDBC_CONNECTION_STRING, host, port, database),
          getConnectionProperties());
    } catch (ClassNotFoundException | SQLException ex) {
      System.out.println("Problem while obtaining the connection.");
      System.out.println(ex.getMessage());
      throw ex;
    }
  }

  /*
   * Returns Database host name for JDBC connection
   */
  public String getHost() {
    return host;
  }

  /*
   * Returns Database port for JDBC connection
   */
  public Integer getPort() {
    return port;
  }

  /*
   * Returns Credentials database
   */
  public Credentials getCredentials() {
    return credentials;
  }

  /*
   * Returns Database name for JDBC connection if present
   */
  public String getDatabase() {
    return database;
  }

  public static final class Builder {

    private static final Integer DEFAULT_PORT = 1521;
    String host;
    Integer port = DEFAULT_PORT;
    String database;
    Integer sessions;
    Credentials credentials;

    private Builder() {
    }

    public Builder withHost(String host) {
      this.host = host;
      return this;
    }

    public Builder withPort(Integer port) {
      this.port = port != null ? port : DEFAULT_PORT;
      return this;
    }

    public Builder withDatabase(String database) {
      this.database = database;
      return this;
    }

    public Builder withCredentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    // TODO: felipegc this is not being used for anything, validate what it is.
    public Builder withMaxSessions(int sessions) {
      this.sessions = sessions;
      return this;
    }

    public OjdbcConnector build() {
      return new OjdbcConnector(
          host, port, database, sessions, credentials);
    }
  }
}
