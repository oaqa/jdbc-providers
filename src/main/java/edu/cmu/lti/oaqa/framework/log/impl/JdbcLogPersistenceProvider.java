/*
 *  Copyright 2012 Carnegie Mellon University
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.cmu.lti.oaqa.framework.log.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.springframework.jdbc.core.PreparedStatementSetter;

import com.google.common.collect.Lists;

import edu.cmu.lti.oaqa.ecd.log.LogEntry;
import edu.cmu.lti.oaqa.ecd.persistence.AbstractLogPersistenceProvider;
import edu.cmu.lti.oaqa.ecd.phase.Trace;
import edu.cmu.lti.oaqa.framework.DataStoreImpl;

public class JdbcLogPersistenceProvider extends AbstractLogPersistenceProvider {

  @Override
  public void log(final String uuid, final Trace trace, final LogEntry type, final String message) {
    System.out.printf("[logger] %s,%s,%s,%s,%s\n", new Date(), uuid, trace, type, message);
    String insert = (String) getParameterValue("insert-log-entry-query");
    DataStoreImpl.getInstance().jdbcTemplate().update(insert, new PreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, uuid);
        ps.setString(2, trace.getTraceHash());
        ps.setString(3, type.toString());
        ps.setString(4, message);
      }
    });
  }

  public List<String> getLogEntries(final String uuid, final Trace trace, final LogEntry type) {
    String query = getSelectLogEntriesQuery(trace, type);
    List<String> params = Lists.newArrayList(uuid);
    if (trace != null) {
      params.add(trace.getTraceHash());
    }
    if (type != null) {
      params.add(type.toString());
    }
    return DataStoreImpl.getInstance().jdbcTemplate().queryForList(query, String.class, params.toArray());
  }
  
  private String getSelectLogEntriesQuery(Trace trace, LogEntry type) {
    StringBuilder query = new StringBuilder();
    query.append((String) getParameterValue("select-log-entries-query"));
    if (trace != null) {
      query.append((String) getParameterValue("select-log-entries-trace-clause"));
    }
    if (type != null) {
      query.append((String) getParameterValue("select-log-entries-type-clause"));
    }
    return query.toString();
  }
}
