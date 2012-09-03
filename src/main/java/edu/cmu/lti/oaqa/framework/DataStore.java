package edu.cmu.lti.oaqa.framework;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public interface DataStore {
  JdbcTemplate jdbcTemplate();
  NamedParameterJdbcTemplate namedJdbcTemplate();
  @Deprecated
  boolean isEmbedded();
}
