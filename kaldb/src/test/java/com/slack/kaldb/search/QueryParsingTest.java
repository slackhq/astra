package com.slack.kaldb.search;

import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class QueryParsingTest extends LuceneTestCase  {

    public Directory dir;

    @Before
    public void before() throws Exception {
        dir = newDirectory();
    }

    @After
    public void after() throws Exception {
        dir.close();
    }

    @Test
    public void testRangeFacet() throws Exception {
        IndexWriter w =
                new IndexWriter(dir, newIndexWriterConfig().setCodec(new SimpleTextCodec()));

        Document doc = new Document();
        doc.add(new IntPoint("year", 2010));
        w.addDocument(doc);
        w.commit();

        DirectoryReader r = DirectoryReader.open(dir);
        IndexSearcher indexSearcher = newSearcher(r, false);

        StandardQueryParser parser = new StandardQueryParser();
        Map<String, PointsConfig> pointsConfig = new HashMap<>();
        pointsConfig.put(
                "year", new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), Integer.class));
        parser.setPointsConfigMap(pointsConfig);


        QueryParser qp = new QueryParser("field", new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
        Query query = parser.parse("year:[2000 TO 2020]", "defaultField");
        assertEquals(1, indexSearcher.count(query));

        r.close();
        w.close();

    }
}
