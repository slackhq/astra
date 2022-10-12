package com.slack.kaldb.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StringFacetLocalTest extends LuceneTestCase {

    private final FacetsConfig config = new FacetsConfig();

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
    // Open question 1 - one needs to preserve facet config and pass it down.
    // Would we need to serialize this and write it out with the index? Why doesn't Lucene automatically write it out?
    public void testSingleFieldFacet() throws Exception {
        indexData(100);
        DirectoryReader indexReader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        SortedSetDocValuesReaderState state =
                new DefaultSortedSetDocValuesReaderState(indexReader, config);

        // Aggregates the facet counts
        FacetsCollector fc = new FacetsCollector();

        // MatchAllDocsQuery is for "browsing" (counts facets
        // for all non-deleted docs in the index); normally
        // you'd use a "normal" query:
        FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc);

        // Retrieve results
        Facets facets = new SortedSetDocValuesFacetCounts(state, fc);

        List<FacetResult> results = new ArrayList<>();
        results.add(facets.getTopChildren(10, "Author"));
        assertEquals(1, results.size());
        assertEquals("Author", results.get(0).dim);
        assertEquals(100, results.get(0).value);

        indexReader.close();
    }

    private void indexData(int nDocs) throws Exception {
        IndexWriter w =
                new IndexWriter(dir, newIndexWriterConfig());

        for (int i=0 ; i<nDocs; i++) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesFacetField("Author", "Bob"));
            w.addDocument(config.build(doc));

            if (usually()) {
                w.commit();
            }
        }
        w.close();
    }
}
