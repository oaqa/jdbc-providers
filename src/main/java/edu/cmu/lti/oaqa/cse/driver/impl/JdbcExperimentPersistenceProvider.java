package edu.cmu.lti.oaqa.cse.driver.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;

import com.google.common.base.Preconditions;

import edu.cmu.lti.oaqa.ecd.persistence.AbstractExperimentPersistenceProvider;
import edu.cmu.lti.oaqa.framework.DataStoreImpl;

public class JdbcExperimentPersistenceProvider extends AbstractExperimentPersistenceProvider {

  private static final String NON_NULL_ERROR_MSG = "Parameter <%s> must not be null";
  
  @Override
  public boolean initialize(ResourceSpecifier aSpecifier, Map<String, Object> tuples)
          throws ResourceInitializationException {
    try {
      String url = (String) tuples.get("url");
      Preconditions.checkNotNull(url, NON_NULL_ERROR_MSG, "url");
      String username = (String) tuples.get("username");
      Preconditions.checkNotNull(username, NON_NULL_ERROR_MSG, "username");
      String password = (String) tuples.get("password");
      Preconditions.checkNotNull(password, NON_NULL_ERROR_MSG, "password");
      DataStoreImpl.getInstance(url, username, password);
      return true;
    } catch (SQLException e) {
      throw new ResourceInitializationException(e);
    }
  }

  @Override
  public void insertExperiment(final String id, final String name, final String author,
          final String configuration, final String resource) throws Exception {
    String insert = (String) getParameterValue("insert-experiment-query");
    JdbcTemplate jdbcTemplate = DataStoreImpl.getInstance().jdbcTemplate();
    jdbcTemplate.update(insert, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, id);
        ps.setString(2, name);
        ps.setString(3, author);
        ps.setString(4, configuration);
        ps.setString(5, resource);
      }
    });
  }

  @Override
  public void updateExperimentMeta(final String experimentId, final int size) {
    String insert = (String) getParameterValue("update-experiment-meta-query");
    JdbcTemplate jdbcTemplate = DataStoreImpl.getInstance().jdbcTemplate();
    jdbcTemplate.update(insert, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setInt(1, size);
        ps.setString(2, experimentId);
      }
    }); 
  }
}
