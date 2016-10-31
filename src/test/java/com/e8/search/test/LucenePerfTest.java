package com.e8.search.test;


import com.e8.search.util.DataLoader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LucenePerfTest {
    @Test
    public void testLuceneStandardAnalyzer() throws Exception {
        Analyzer analyzer = new StandardAnalyzer();
        // Store the index in memory:
        Directory directory = new RAMDirectory();
        DataLoader.loadLuceneIndexFromCSV("data_10M.csv", analyzer, directory);
        DirectoryReader ireader = DirectoryReader.open(directory);
        System.out.println("Number of docs indexed: "+ ireader.numDocs());
        IndexSearcher isearcher = new IndexSearcher(ireader);
        testIpAddress(isearcher, analyzer);
        testHostName(isearcher, analyzer);
        testMacAddress(isearcher, analyzer);

        ireader.close();
        directory.close();
    }
    private void testIpAddress(IndexSearcher iSearcher, Analyzer analyzer) throws Exception{
        //QueryParser parser = new QueryParser("ipAddress", analyzer);
        //Query query = parser.parse("1");
        RegexpQuery regexpQuery = new RegexpQuery(new Term( "ipAddress", ".*"+"1"+".*"));
        long start = System.nanoTime();
        ScoreDoc[] hits = iSearcher.search(regexpQuery, 10).scoreDocs;
        long stop = System.nanoTime();
        System.out.println("Time taken for fetching the hits: "+(stop-start)/(1000)+" micro secs");
        assertEquals(10, hits.length);

        for(ScoreDoc hit : hits){
            Document hitDoc = iSearcher.doc(hit.doc);
            System.out.println("Doc: "+ hitDoc.toString());
            assertTrue(hitDoc.get("ipAddress").contains("1"));
        }

        start = System.nanoTime();
        for(int i=0; i<100; i++) {
            hits = iSearcher.search(regexpQuery, 10).scoreDocs;
        }
        stop = System.nanoTime();
        System.out.println("Avg Time taken for fetching the hits: "+(stop-start)/(1000*100)+" micro secs");

        assertEquals(10, hits.length);
    }

    private void testHostName(IndexSearcher isearcher, Analyzer analyzer) throws Exception{
        //QueryParser parser = new QueryParser("hostName", analyzer);
        //Query query = parser.parse("a");
        RegexpQuery regexpQuery = new RegexpQuery(new Term( "hostName", ".*"+"a"+".*"));
        long start = System.nanoTime();
        ScoreDoc[] hits = isearcher.search(regexpQuery, 10).scoreDocs;
        long stop = System.nanoTime();
        System.out.println("Time taken for fetching the hits: "+(stop-start)/(1000)+" micro secs");
        assertEquals(10, hits.length);

        for(ScoreDoc hit : hits){
            Document hitDoc = isearcher.doc(hit.doc);
            System.out.println("Doc: "+ hitDoc.toString());
            assertTrue(hitDoc.get("hostName").startsWith("a")
                    || hitDoc.get("hostName").startsWith("A")
                    || hitDoc.get("hostName").contains("A")
                    || hitDoc.get("hostName").contains("a"));
        }

        start = System.nanoTime();
        for(int i=0; i<100; i++) {
            hits = isearcher.search(regexpQuery, 10).scoreDocs;
        }
        stop = System.nanoTime();
        System.out.println("Avg Time taken for fetching the hits: "+(stop-start)/(1000*100)+" micro secs");

        assertEquals(10, hits.length);
    }

    private void testMacAddress(IndexSearcher isearcher, Analyzer analyzer) throws Exception{
        //QueryParser parser = new QueryParser("macAddress", analyzer);
        //Query query = parser.parse("a*");
        RegexpQuery regexpQuery = new RegexpQuery(new Term( "macAddress", ".*"+"a"+".*"));
        long start = System.nanoTime();
        ScoreDoc[] hits = isearcher.search(regexpQuery, 10).scoreDocs;
        long stop = System.nanoTime();
        System.out.println("Time taken for fetching the hits: "+(stop-start)/(1000)+" micro secs");
        assertEquals(10, hits.length);

        for(ScoreDoc hit : hits){
            Document hitDoc = isearcher.doc(hit.doc);
            System.out.println("Doc: "+ hitDoc.toString());
            assertTrue(hitDoc.get("macAddress").startsWith("a")
                    || hitDoc.get("macAddress").startsWith("A")
                    || hitDoc.get("macAddress").contains("A")
                    || hitDoc.get("macAddress").contains("a"));
        }

        start = System.nanoTime();
        for(int i=0; i<100; i++) {
            hits = isearcher.search(regexpQuery, 10).scoreDocs;
        }
        stop = System.nanoTime();
        System.out.println("Avg Time taken for fetching the hits: "+(stop-start)/(1000*100)+" micro secs");

        assertEquals(10, hits.length);
    }
}
