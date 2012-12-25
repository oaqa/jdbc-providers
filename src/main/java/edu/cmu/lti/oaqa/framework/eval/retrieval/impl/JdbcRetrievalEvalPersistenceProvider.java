package edu.cmu.lti.oaqa.framework.eval.retrieval.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

import edu.cmu.lti.oaqa.ecd.phase.Trace;
import edu.cmu.lti.oaqa.framework.DataStoreImpl;
import edu.cmu.lti.oaqa.framework.eval.ExperimentKey;
import edu.cmu.lti.oaqa.framework.eval.Key;
import edu.cmu.lti.oaqa.framework.eval.retrieval.FMeasureEvaluationData;
import edu.cmu.lti.oaqa.framework.eval.retrieval.RetrievalCounts;
import edu.cmu.lti.oaqa.framework.persistence.AbstractRetrievalEvalPersistenceProvider;

public class JdbcRetrievalEvalPersistenceProvider extends AbstractRetrievalEvalPersistenceProvider {
  
  @Override
  public void insertPartialCounts(final Key key, final String sequenceId, final RetrievalCounts counts) throws SQLException {
    final String eName = getClass().getSimpleName();
    String insert = (String) getParameterValue("insert-passage-aggregates-query");
    final Trace trace = key.getTrace();
    DataStoreImpl.getInstance().jdbcTemplate().update(insert, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, key.getExperiment());
        ps.setString(2, trace.getTrace());
        ps.setString(3, eName);
        ps.setFloat(4, counts.getRelevantRetrieved());
        ps.setFloat(5, counts.getRetrieved());
        ps.setFloat(6, counts.getRelevant());
        ps.setFloat(7, counts.getAvep());
        ps.setFloat(8, counts.getCount());
        ps.setInt(9, counts.getBinaryRelevant());
        ps.setString(10, sequenceId); // TODO: JDBC OAQA
        ps.setInt(11, key.getStage());
        ps.setString(12, trace.getTraceHash());
      }
    });
  }

  @Override
  public void deletePassageAggrEval(final Key key, final String sequenceId) {    
    final String name = getClass().getSimpleName();
    String insert = (String) getParameterValue("delete-passage-aggr-eval-query");
    DataStoreImpl.getInstance().jdbcTemplate().update(insert, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, key.getExperiment());
        ps.setString(2, key.getTrace().getTraceHash());
        ps.setString(3, name);
        ps.setString(4, sequenceId); // TODO: JDBC OAQA
      }
    });
  }
  
  @Override
  public Multimap<Key, RetrievalCounts> retrievePartialCounts(final ExperimentKey experiment) {
    String select = (String) getParameterValue("select-passage-aggregates-query");
    final Multimap<Key, RetrievalCounts> counts = LinkedHashMultimap.create();
    RowCallbackHandler handler = new RowCallbackHandler() {
      public void processRow(ResultSet rs) throws SQLException {
        Key key = new Key(rs.getString("experimentId"), new Trace(rs.getString("traceId")), rs.getInt("stage"));
        RetrievalCounts cnt = new RetrievalCounts(rs.getInt("relevant_retrieved"), rs.getInt("retrieved"),
                rs.getInt("relevant"), rs.getFloat("avep"), rs.getInt("binary_relevant"),
                rs.getInt("count"));
        counts.put(key, cnt);
      }
    };
    DataStoreImpl.getInstance().jdbcTemplate().query(select, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, experiment.getExperiment());
        ps.setInt(2, experiment.getStage());
      }
    }, handler);
    return counts;
  }
  
  @Override
  public void insertFMeasureEval(final Key key, final String eName,
          final FMeasureEvaluationData eval) throws SQLException {
    String insert = (String) getParameterValue("insert-f-measure-eval-query");
    final Trace trace = key.getTrace();
    DataStoreImpl.getInstance().jdbcTemplate().update(insert, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, key.getExperiment());
        ps.setString(2, trace.getTrace());
        ps.setString(3, eName);
        ps.setFloat(4, eval.getPrecision());
        ps.setFloat(5, eval.getRecall());
        ps.setFloat(6, eval.getF1());
        ps.setFloat(7, eval.getMap());
        ps.setFloat(8, eval.getBinaryRecall());
        ps.setInt(9, eval.getCount());
        ps.setInt(10, key.getStage());
        ps.setString(11, trace.getTraceHash());
      }
    });
  }

  @Override
  public void deleteFMeasureEval(final ExperimentKey experiment) {
    String insert = (String) getParameterValue("delete-f-measure-eval-query");
    DataStoreImpl.getInstance().jdbcTemplate().update(insert, new PreparedStatementSetter() {
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, experiment.getExperiment());
        ps.setInt(2, experiment.getStage());
      }
    });
  }
}