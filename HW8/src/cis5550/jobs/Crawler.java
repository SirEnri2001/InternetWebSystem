package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import javax.naming.Context;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

public class Crawler {
    static FlameRDD urlQueue;
    private static String[] parseHtmlTag(String tagString){
        tagString = tagString.substring(1, tagString.length()-1).trim();
        return tagString.split("[ =\"]");
    }
    private static String getUrlFromATag(String tagString) {
        String[] strings = parseHtmlTag(tagString);
        if(strings[0].compareToIgnoreCase("a")!=0){
            return null;
        }
        String href = null;
        for(int j = 1;j< strings.length;j+=2){
            if(strings[j].compareToIgnoreCase("href")==0){
                href = strings[j+1];
                break;
            }
        }
        return href;
    }
    public static List<String> parseHtml(InputStream htmlStream) throws IOException {
        StringBuilder tagBuilder = new StringBuilder();
        LinkedList<String> hrefs = new LinkedList<>();
        boolean inTag = false;
        boolean inString = false;
        int i = 0;
        while((i=htmlStream.read())!=0){
            if(!inTag && i=='<'){
                inTag = true;
            }
            if(inTag){
                tagBuilder.append((char)i);
            }
            if(inTag && i=='"'){
                inString = !inString;
            }
            if(inTag && i=='>'){
                inTag = false;
                String tagString = tagBuilder.toString();
                String href = getUrlFromATag(tagString);
                if(href!=null){
                    hrefs.add(href);
                }
            }
        }
        return hrefs;
    }
    public static void run(FlameContext flameContext, String[] seedUrls) {
        try{
            String tableName = "pt-crawl";
            if(seedUrls.length==0){
                flameContext.output("No element found!");
                return;
            }
            LinkedList<String> list = new LinkedList<String>();
            for (int i=0; i<seedUrls.length; i++)
                list.add(seedUrls[i]);
            FlameRDD.StringToIterable lambdaFunction = (String urlString)->{
                try{
                    flameContext.output("In lambda function");
                    URL url = new URL(urlString);
                    HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
                    httpURLConnection.setRequestMethod("GET");
                    httpURLConnection.setRequestProperty("","");
                    httpURLConnection.connect();
                    if(httpURLConnection.getResponseCode()!=200){
                        return new ArrayList<>();
                    }
                    Row row = new Row(Hasher.hash(urlString));
                    flameContext.getKVS().putRow(tableName, row);
                    List<String> hrefs = parseHtml(httpURLConnection.getInputStream());
                    for(String s : hrefs){
                        flameContext.output(s);
                    }
                    return hrefs;
                }catch (Exception e){
                    for(StackTraceElement element : e.getStackTrace()){
                        flameContext.output(element.toString());
                        flameContext.output("\n");
                    }
                }
                return new ArrayList<String>();
            };
            urlQueue = flameContext.parallelize(list);
            urlQueue.saveAsTable("aaaa");
            while(urlQueue.count()!=0) {
                urlQueue = urlQueue.flatMap(lambdaFunction);
                break;
            }
            flameContext.output("OK");
            urlQueue.saveAsTable("resultTable");
            return;
        }catch (Exception e){
            flameContext.output(e.toString());
            for(StackTraceElement element : e.getStackTrace()){
                flameContext.output(element.toString());
                flameContext.output("\n");
            }
        }
    }



}
