package edu.cmu.lti.oaqa.cse.driver.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;

import com.google.common.base.Preconditions;

import edu.cmu.lti.oaqa.ecd.impl.AbstractGroupExperimentPersistenceProvider;
import edu.cmu.lti.oaqa.framework.DataStoreImpl;

public class JdbcGroupExperimentPersistenceProvider extends AbstractGroupExperimentPersistenceProvider {

  private static final String NON_NULL_ERROR_MSG = "Parameter <%s> must not be null";

  @Override
  public boolean initialize(ResourceSpecifier aSpecifier, Map<String, Object> tuples)
          throws ResourceInitializationException {
    try {
      super.initialize(aSpecifier, tuples);
      String url = (String) tuples.get("url");      
      Preconditions.checkNotNull(url, NON_NULL_ERROR_MSG, "url");
      String username = (String) tuples.get("username");
      String password = (String) tuples.get("password");
      String driver = (String) tuples.get("driver");
      DataStoreImpl.getInstance(url, username, password, driver);
      return true;
    } catch (SQLException e) {
      throw new ResourceInitializationException(e);
    }
  }
  
  @Override
  public void insertGroupExperiment(final String id, final String fold, final String foldId, final String name,
          final String author, final String configuration, final String resource, final String testInstanceIdx)
          throws Exception {
    String insert = (String) getParameterValue("insert-group-experiment-query");
    JdbcTemplate jdbcTemplate = DataStoreImpl.getInstance().jdbcTemplate();
    jdbcTemplate.update(insert, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, id);
        ps.setString(2, fold);
        ps.setString(3, foldId);
        ps.setString(4, name);
        ps.setString(5, author);
        ps.setString(6, configuration);
        ps.setString(7, resource);
        ps.setString(8, testInstanceIdx);
      }
    });
  }
  
  
}
