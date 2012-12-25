package edu.cmu.lti.oaqa.framework.collection.impl;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import edu.cmu.lti.oaqa.framework.DataElement;

public final class DataElementRowMapper implements RowMapper<DataElement> {
  @Override
  public DataElement mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new DataElement(rs.getString("dataset"), rs.getString("sequenceId"),
            rs.getString("question"), null);
  }
}