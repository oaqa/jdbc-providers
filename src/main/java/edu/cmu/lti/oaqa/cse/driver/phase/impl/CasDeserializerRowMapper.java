package edu.cmu.lti.oaqa.cse.driver.phase.impl;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;

import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.jcas.JCas;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;

import edu.cmu.lti.oaqa.ecd.phase.CasDeserializer;

public final class CasDeserializerRowMapper implements CasDeserializer, RowCallbackHandler {
  private final JCas nextCas;

  private final LobHandler lobHandler;
  
  private boolean processedCas = false;

  public CasDeserializerRowMapper(JCas nextCas) {
    this.nextCas = nextCas;
    this.lobHandler = new DefaultLobHandler();;
  }

  @Override
  public void processRow(ResultSet rs) throws SQLException {
    InputStream in = lobHandler.getBlobAsBinaryStream(rs, "xcas");
    try {
      GZIPInputStream gz = new GZIPInputStream(in);
      nextCas.reset();
      XmiCasDeserializer.deserialize(gz, nextCas.getCas(), true);
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