package edu.cmu.lti.oaqa.cse.driver.impl;

import java.util.Set;

import org.springframework.jdbc.core.JdbcTemplate;
import org.yaml.snakeyaml.Yaml;

import edu.cmu.lti.oaqa.framework.DataStoreImpl;
import edu.cmu.lti.oaqa.framework.persistence.AbstractAsyncConfiguration;

public class JdbcAsyncConfigPersistenceProvider extends AbstractAsyncConfiguration {

  @Override
  public Set<Integer> getPublishedTopics(String experiment) {
    String query = (String) getParameterValue("experiment-topics-query");
    JdbcTemplate template = DataStoreImpl.getInstance().jdbcTemplate();
    String topicsSerial = template.queryForObject(query, String.class, experiment);
    Yaml yaml = new Yaml();
    @SuppressWarnings("unchecked")
    Set<Integer> topics = (Set<Integer>) yaml.load(topicsSerial);
    return topics;
  }
}
