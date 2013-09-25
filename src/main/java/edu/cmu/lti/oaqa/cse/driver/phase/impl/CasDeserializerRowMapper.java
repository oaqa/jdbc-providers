package edu.cmu.lti.oaqa.cse.driver.phase.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.uima.jcas.JCas;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;

import edu.cmu.lti.oaqa.ecd.phase.BasePhase;
import edu.cmu.lti.oaqa.ecd.phase.CasDeserializer;

public final class CasDeserializerRowMapper implements CasDeserializer, RowCallbackHandler {
  private final JCas nextCas;

  private final LobHandler lobHandler;
  
  private boolean processedCas = false;

  public CasDeserializerRowMapper(JCas nextCas) {
    this.nextCas = nextCas;
    this.lobHandler = new DefaultLobHandler();
  }

  @Override
  public void processRow(ResultSet rs) throws SQLException {
    // InputStream in = lobHandler.getBlobAsBinaryStream(rs, "xcas");
    // Work with sqlite jdbc driver
    InputStream in = new ByteArrayInputStream(lobHandler.getBlobAsBytes(rs, "xcas"));
    try {
      nextCas.reset();
      BasePhase.deserializeCAS(nextCas.getCas(), in);
      processedCas = true;
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public boolean processedCas() {
    return processedCas;
  }
} 
