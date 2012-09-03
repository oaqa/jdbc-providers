package edu.cmu.lti.oaqa.cse.driver.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;

import edu.cmu.lti.oaqa.ecd.persistence.AbstractExperimentPersistenceProvider;
import edu.cmu.lti.oaqa.framework.DataStoreImpl;

public abstract class JdbcExperimentPersistenceProvider extends AbstractExperimentPersistenceProvider {

  @Override
  public boolean initialize(ResourceSpecifier aSpecifier, Map<String, Object> tuples)
          throws ResourceInitializationException {
    try {
      String url = (String) tuples.get("url");
      String username = (String) tuples.get("username");
      String password = (String) tuples.get("password");
      Boolean embedded = (Boolean) tuples.get("embedded");
      DataStoreImpl.getInstance(url, username, password, embedded != null ? embedded.booleanValue()
              : false);
      return true;
    } catch (SQLException e) {
      throw new ResourceInitializationException(e);
    }
  }

  @Override
  public void insertExperiment(final String id, final String name, final String author,
          final String configuration, final String resource) throws Exception {
    String insert = getInsertExperimentQuery();
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
    String insert = getInsertMetaQuery();
    JdbcTemplate jdbcTemplate = DataStoreImpl.getInstance().jdbcTemplate();
    jdbcTemplate.update(insert, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setInt(1, size);
        ps.setString(2, experimentId);
      }
    }); 
  }
  
  protected abstract String getInsertMetaQuery();
  
  protected abstract String getInsertExperimentQuery();
}
