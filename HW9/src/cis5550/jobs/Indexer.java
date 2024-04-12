package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.kvs.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class Indexer {
    public static void run(FlameContext flameContext, String[] seedUrls) throws Exception {
        FlamePairRDD pairRDD = flameContext.fromTable("pt-crawl", (Row row)->{
            return row.get("url") + "|" + row.get("page");
        }).mapToPair((String s)->{
            return new FlamePair(s.split("\\|")[0],s.split("\\|")[1]);
        });
        pairRDD.flatMapToPair((FlamePair pair)->{
            String html = pair._2().toLowerCase();
            html = html.replaceAll("<.*?>", " ");
            html = html.replaceAll("\\W+", " ");
            LinkedList<FlamePair> pairs = new LinkedList<>();
            HashMap<String, Integer> wordMap = new HashMap<>();
            for(String word : html.split(" ")){
                if(!wordMap.containsKey(word)){
                    wordMap.put(word, 1);
                }else{
                    continue;
                }
                pairs.add(new FlamePair(word, pair._1()));
            }
            return pairs;
        }).foldByKey("", (String s1, String s2)->{
            if(s1.isEmpty()){
                return s2;
            }
            if(s2.isEmpty()){
                return s1;
            }
            return s1 + "," + s2;
        }).saveAsTable("pt-index");
    }
}
