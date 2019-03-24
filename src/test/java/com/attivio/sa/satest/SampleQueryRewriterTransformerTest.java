/**
* Copyright 2015 Attivio Inc., All rights reserved.
*/
package com.attivio.sa.satest;

import org.junit.Assert;
import org.junit.Test;

import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.query.JoinQuery;
import com.attivio.sdk.search.query.PhraseQuery;
import com.attivio.sa.satest.SampleQueryRewriteTransformer;

public class SampleQueryRewriterTransformerTest {

  @Test
  public void testBasics() throws Exception {
    SampleQueryRewriteTransformer trans = new SampleQueryRewriteTransformer();
    QueryRequest req = new QueryRequest(new PhraseQuery("authors", "mike"));
    trans.processQuery(req);
    System.err.println(req.getQuery());
    Assert.assertTrue(req.getQuery() instanceof JoinQuery);
  }

}
