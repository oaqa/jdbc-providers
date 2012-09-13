package edu.cmu.lti.oaqa.framework;

import java.sql.SQLException;

import org.apache.uima.UimaContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public final class DataStoreImpl implements DataStore {

  private final JdbcTemplate jdbcTemplate;

  private final NamedParameterJdbcTemplate namedJdbcTemplate;

  private static DataStore ds;

  public static DataStore getInstance() {
    if (ds == null) {
      throw new IllegalStateException("DataStore has not been initialized");
    }
    return ds;
  }

  public static DataStore getInstance(UimaContext c) throws SQLException {
    String url = (String) c.getConfigParameterValue("url");
    String username = (String) c.getConfigParameterValue("username");
    String password = (String) c.getConfigParameterValue("password");
    return getInstance(url, username, password);
  }

  public static DataStore getInstance(String url, String username, String password)
          throws SQLException {
    return getInstance(url, username, password, false);
  }

  public static DataStore getInstance(String url, String username, String password, String driver)
          throws SQLException {
    if (ds == null) {
      ds = new DataStoreImpl(url, username, password, driver);
    }
    return ds;
  }

  @Deprecated
  public static DataStore getInstance(String url, String username, String password, boolean embedded)
          throws SQLException {
    if (ds == null) {
      ds = new DataStoreImpl(url, username, password, null);
    }
    return ds;
  }

  public static void setMock(DataStore _ds) {
    ds = _ds;
  }

  private DataStoreImpl(String url, String username, String password, String driver) throws SQLException {
    // System.setProperty("sqlite.purejava", "true");
    if (driver != null) {
      try {
        Class.forName("org.sqlite.JDBC");
      } catch (ClassNotFoundException e) {
        throw new SQLException(e);
      }
    }
    ComboPooledDataSource cpds = new ComboPooledDataSource();
    cpds.setJdbcUrl(url);
    if (username != null) {
      cpds.setUser(username);
    }
    if (password != null) {
      cpds.setPassword(password);
    }
    cpds.setPreferredTestQuery("SELECT 1");
    // cpds.setTestConnectionOnCheckout(!embedded);
    cpds.setInitialPoolSize(1);
    cpds.setMinPoolSize(1);
    cpds.setMaxPoolSize(8); //  Allow for timedout objects
    this.jdbcTemplate = new JdbcTemplate(cpds);
    this.namedJdbcTemplate = new NamedParameterJdbcTemplate(cpds);
  }

  @Override
  public JdbcTemplate jdbcTemplate() {
    return jdbcTemplate;
  }

  @Override
  public NamedParameterJdbcTemplate namedJdbcTemplate() {
    return namedJdbcTemplate;
  }

  @Deprecated
  @Override
  public boolean isEmbedded() {
    return false;
  }

}
