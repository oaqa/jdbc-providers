package edu.cmu.lti.oaqa.cse.driver.phase.impl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.uima.jcas.JCas;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;
import org.springframework.jdbc.support.lob.LobHandler;
import org.xml.sax.SAXException;

import edu.cmu.lti.oaqa.ecd.impl.AbstractPhasePersistenceProvider;
import edu.cmu.lti.oaqa.ecd.phase.CasDeserializer;
import edu.cmu.lti.oaqa.ecd.phase.ExecutionStatus;
import edu.cmu.lti.oaqa.framework.DataStoreImpl;

public class JdbcPhasePersistenceProvider extends AbstractPhasePersistenceProvider {

  @Override
  public void insertExecutionTrace(final String optionId, final String question,
          final String sequenceId, final String dataset, final Integer phaseNo, final String uuid,
          final long startTime, final String hostname, final String trace, final String key)
          throws IOException {
    DataStoreImpl.getInstance().jdbcTemplate().update(new PreparedStatementCreator() {
      @Override
      public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
        String insert = (String) getParameterValue("insert-query");
        PreparedStatement ps = con.prepareStatement(insert);
        ps.setString(1, optionId);
        ps.setString(2, question);
        ps.setString(3, sequenceId); // TODO: JDBC OAQA
        ps.setString(4, dataset);
        ps.setInt(5, phaseNo);
        ps.setString(6, uuid);
        ps.setLong(7, startTime);
        // TODO: Fix this with the key string... Maybe is no longer required
        ps.setLong(8, -1); // Prev CAS
        ps.setString(9, hostname);
        ps.setString(10, trace);
        ps.setString(11, key);
        return ps;
      }
    });
  }

  @Override
  public void storeCas(final byte[] bytes, final ExecutionStatus status, final long endTime,
          final String key) throws IOException {
    String update = (String) getParameterValue("update-query");
    LobHandler lobHandler = new DefaultLobHandler();
    DataStoreImpl.getInstance().jdbcTemplate()
            .execute(update, new AbstractLobCreatingPreparedStatementCallback(lobHandler) {
              protected void setValues(PreparedStatement ps, LobCreator lobCreator)
                      throws SQLException {
                lobCreator.setBlobAsBytes(ps, 1, bytes);
                ps.setString(2, status.toString());
                ps.setLong(3, endTime);
                ps.setString(4, key);
              }
            });
  }

  @Override
  public void storeException(final byte[] bytes, final ExecutionStatus status, final long endTime,
          final String key) throws IOException, SAXException {
    String update = (String) getParameterValue("update-query");
    LobHandler lobHandler = new DefaultLobHandler();
    DataStoreImpl.getInstance().jdbcTemplate()
            .execute(update, new AbstractLobCreatingPreparedStatementCallback(lobHandler) {
              protected void setValues(PreparedStatement ps, LobCreator lobCreator)
                      throws SQLException {
                lobCreator.setBlobAsBytes(ps, 1, bytes);
                ps.setString(2, status.toString());
                ps.setLong(3, endTime);
                ps.setString(4, key);
              }
            });
  }

  @Override
  public CasDeserializer deserialize(JCas jcas, final String hash) throws SQLException {
    CasDeserializerRowMapper mapper = new CasDeserializerRowMapper(jcas);
    String select = (String) getParameterValue("select-query");
    DataStoreImpl.getInstance().jdbcTemplate().query(select, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, hash);
        ps.setString(2, ExecutionStatus.FAILURE.toString());
      }
    }, mapper);
    return mapper;
  }

  @Override
  public void insertExperimentMeta(final String experimentId, final int phaseNo, final int stageId,
          final int size) {
    String insert = (String) getParameterValue("insert-meta-query");
    JdbcTemplate jdbcTemplate = DataStoreImpl.getInstance().jdbcTemplate();
    jdbcTemplate.update(insert, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, experimentId);
        ps.setInt(2, phaseNo);
        ps.setInt(3, stageId);
        ps.setInt(4, size);
      }
    });
  }
}