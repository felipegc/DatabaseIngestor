package com.magicbq.databaseingestor.objects;

public class Credentials {
  private final String username;
  private final String password;

  private Credentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  /**
   * Build for Credentials.
   */
  public static Builder newBuilder() {
    return new Credentials.Builder();
  }

  public static final class Builder {

    private static final Integer DEFAULT_PORT = 1521;
    String username;
    String password;

    private Builder() {
    }

    public Builder withUsername(String username) {
      this.username = username;
      return this;
    }

    public Builder withPassword(String password) {
      this.password = password;
      return this;
    }


    public Credentials build() {
      return new Credentials(username, password);
    }
  }
}
