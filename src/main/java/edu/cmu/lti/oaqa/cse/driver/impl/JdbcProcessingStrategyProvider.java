package edu.cmu.lti.oaqa.cse.driver.impl;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

import edu.cmu.lti.oaqa.ecd.flow.FunneledFlow;
import edu.cmu.lti.oaqa.ecd.funnel.SetBasedFunnel;
import edu.cmu.lti.oaqa.ecd.impl.AbstractFunnelingStrategy;
import edu.cmu.lti.oaqa.framework.DataStoreImpl;

public class JdbcProcessingStrategyProvider extends AbstractFunnelingStrategy {
  
  @Override
  public FunneledFlow newFunnelStrategy(String experimentUuid) {
    List<String> matchingTraces = getMatchingTraces(experimentUuid);
    return new SetBasedFunnel(matchingTraces);
  }
  
  private List<String> getMatchingTraces(String experiment) {
    String query = (String) getParameterValue("select-funneled-traces");
    JdbcTemplate template = DataStoreImpl.getInstance().jdbcTemplate();
    return template.queryForList(query, String.class, experiment);
  }
}
