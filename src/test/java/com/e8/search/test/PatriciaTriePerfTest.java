package com.e8.search.test;

import com.e8.search.util.DataLoader;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;

public class PatriciaTriePerfTest {
    Map<String, PatriciaTrie<String>> trieMap;
    @Before
    public void setup() throws Exception{
        trieMap = DataLoader.loadTriesFromCSV("data_10M.csv");
    }
    @Test
    public void testTrieNoCopy() throws Exception{
        PatriciaTrie<String> trie = trieMap.get("ipAddress");
        assertNotNull(trie);
        assertNotEquals(0, trie.size());
        //prime the cache
        long start = System.nanoTime();
        Collection<String> all = getAllNoCopy("1", "ipAddress", trieMap);
        long stop = System.nanoTime();
        System.out.println("Time taken for fetching "+Integer.toString(all.size())+" records: "+Long.toString((stop-start)/(1000))+" micro secs (No Priming)");
        long total = 0;
        List<Integer> list = new ArrayList<>();
        for(int i=0; i<100; i++) {
            start = System.nanoTime();
            all = getAllNoCopy("1", "ipAddress", trieMap);
            stop = System.nanoTime();
            total += stop-start;
            list.add(all.size());
            Iterator<String> iter = all.iterator();
            assertTrue(iter.hasNext());
        }
        assertNotEquals(0,all.size());
        System.out.println("Average Time taken for fetching "+Integer.toString(all.size())+" records: "+Long.toString(total/(1000*100))+" micro secs (Priming)");

    }
    @Test
    public void testTrieAllCopy() throws Exception{
        PatriciaTrie<String> trie = trieMap.get("ipAddress");
        assertNotNull(trie);
        assertNotEquals(0, trie.size());
        //prime the cache
        long start = System.nanoTime();
        Collection<String> all = getAllCopy("1", "ipAddress", trieMap);
        long stop = System.nanoTime();
        System.out.println("Time taken for fetching "+Integer.toString(all.size())+" records: "+Long.toString((stop-start)/(1000))+" micro secs (No Priming)");

        long total=0;
        List<Integer> list = new ArrayList<>();
        for(int i=0; i<100; i++) {
            start = System.nanoTime();
            all = getAllCopy("1", "ipAddress", trieMap);
            stop = System.nanoTime();
            total += stop-start;
            list.add(all.size());
            Iterator<String> iter = all.iterator();
            assertTrue(iter.hasNext());
        }
        assertNotEquals(0,all.size());
        System.out.println("Average Time taken for fetching "+Integer.toString(all.size())+" records: "+Long.toString(total/(1000*100))+" micro secs (Priming)");
    }
   // @Test
    public void testTrieHundred() throws Exception{

        PatriciaTrie<String> trie = trieMap.get("ipAddress");
        assertNotNull(trie);
        assertNotEquals(0, trie.size());
        long start = System.nanoTime();
        List<String> ten = get("1", "ipAddress", trieMap,100);
        long stop = System.nanoTime();
        System.out.println("Time taken for fetching "+Integer.toString(ten.size())+" records: "+Long.toString((stop-start)/(1000))+" micro secs (No Priming)");

        long total = 0;
        List<Integer> list = new ArrayList<>();
        for(int i=0; i<100; i++) {
            start = System.nanoTime();
            ten = get("1", "ipAddress", trieMap, 100);
            stop = System.nanoTime();
            total += (stop-start);
            list.add(ten.size());
            Iterator<String> iter = ten.iterator();
            assertTrue(iter.hasNext());
        }
        System.out.println("Average Time taken for fetching "+Integer.toString(ten.size())+" records: "+Long.toString(total/(1000*100))+" micro secs (Priming)");
        assertEquals(100,ten.size());
    }
    //@Test
    public void testTrieTen() throws Exception{
        PatriciaTrie<String> trie = trieMap.get("ipAddress");
        assertNotNull(trie);
        assertNotEquals(0, trie.size());
        long start = System.nanoTime();
        List<String> ten = get("1", "ipAddress", trieMap,10);
        long stop = System.nanoTime();
        System.out.println("Time taken for fetching "+Integer.toString(ten.size())+" records: "+Long.toString((stop-start)/(1000))+" micro secs (No Priming)");

        long total = 0;
        List<Integer> list = new ArrayList<>();
        for(int i=0; i<100; i++) {
            start = System.nanoTime();
            ten = get("1", "ipAddress", trieMap, 10);
            stop = System.nanoTime();
            total += (stop-start);
            list.add(ten.size());
            Iterator<String> iter = ten.iterator();
            assertTrue(iter.hasNext());
        }
        System.out.println("Average Time taken for fetching "+Integer.toString(ten.size())+" records: "+Long.toString(total/(1000*100))+" micro secs (Priming)");

        assertEquals(10,ten.size());
    }
    private List<String> getAllCopy(String incomingString, String fieldName, Map<String, PatriciaTrie<String>> cache)
            throws Exception{

        List<String> list = Lists.newArrayList();
        PatriciaTrie<String> trie = cache.get(fieldName);

        Map<String, String> map = trie.prefixMap(incomingString.toLowerCase());

        list.addAll(map.values());

        return list;
    }

    private Collection<String> getAllNoCopy(String incomingString, String fieldName, Map<String, PatriciaTrie<String>> cache)
            throws Exception{

        PatriciaTrie<String> trie = cache.get(fieldName);

        Map<String, String> map = trie.prefixMap(incomingString.toLowerCase());

        return map.values();
    }

    private List<String> get(String incomingString, String fieldName, Map<String, PatriciaTrie<String>> cache, int num)
            throws Exception{

        List<String> result = Lists.newArrayList();

        PatriciaTrie<String> trie = cache.get(fieldName);

        Map<String, String> map = trie.prefixMap(incomingString.toLowerCase());

        Iterator<String> values = map.values().iterator();
        for(int i=0; values.hasNext() && i<num;i++) {
            result.add(values.next());
        }
        return result;
    }
}
