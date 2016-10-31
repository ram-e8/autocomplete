package com.e8.search.util;


import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class DataLoader {
    public static Map<String, PatriciaTrie<String>> loadTriesFromCSV(String fileName) throws Exception{
        Reader in = new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName));
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().withSkipHeaderRecord().parse(in);

        Map<String, PatriciaTrie<String>> map = new HashMap<>();
        map.put("ipAddress", new PatriciaTrie<>());
        map.put("macAddress", new PatriciaTrie<>());
        map.put("userName", new PatriciaTrie<>());
        map.put("hostName", new PatriciaTrie<>());
        for (CSVRecord record : records) {
            String ip = record.get("ipAddress");
            map.get("ipAddress").put(ip,ip);

            String macAddress = record.get("macAddress");
            map.get("macAddress").put(macAddress, macAddress);

            String hostName = record.get("hostName");
            map.get("hostName").put(macAddress, hostName);

            String userName = record.get("userName");
            map.get("userName").put(macAddress, userName);

        }
        return map;
    }

    public static class EdgeNGramAnalyzer extends Analyzer {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new EdgeNGramTokenizer(1, 5);
            TokenStream filter = new EdgeNGramTokenFilter(tokenizer, 1, 5);
            filter = new LowerCaseFilter(filter);
            return new TokenStreamComponents(tokenizer, filter);
        }
    }

    public static Directory loadLuceneIndexFromCSV(String fileName, Analyzer analyzer, Directory directory) throws Exception {
        Reader in = new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName));
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().withSkipHeaderRecord().parse(in);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter iwriter = new IndexWriter(directory, config);
        for (CSVRecord record : records) {
            String ip = record.get("ipAddress");
            String macAddress = record.get("macAddress");
            String hostName = record.get("hostName");
            String userName = record.get("userName");
            Document doc = new Document();
            doc.add(new Field("ipAddress", ip, StringField.TYPE_STORED));
            doc.add(new Field("hostName", hostName, StringField.TYPE_STORED));
            doc.add(new Field("macAddress", macAddress, StringField.TYPE_STORED));
            doc.add(new Field("userName", userName, StringField.TYPE_STORED));
            iwriter.addDocument(doc);
        }
        iwriter.commit();
        iwriter.close();
        return directory;
    }
}
