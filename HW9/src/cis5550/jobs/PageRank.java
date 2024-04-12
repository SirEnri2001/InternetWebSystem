package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlamePairRDDImpl;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRank {
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
            if(j+2<strings.length && strings[j].compareToIgnoreCase("href")==0){
                href = strings[j+2];
                break;
            }
        }
        return href;
    }
    private static String normalizeUrl(String url, String referNormalized) throws Exception {
        // url [[http[s]://][<domain_name>[:port_number]]][/]path_name[/path_name]*[#element][?queryparams]
        url = url.trim();
        if(url.indexOf('?')!=-1){
            url = url.substring(0, url.indexOf('?'));
        }
        Pattern protocol = Pattern.compile("https?://", Pattern.CASE_INSENSITIVE);
        Pattern httpProtocol = Pattern.compile("http://", Pattern.CASE_INSENSITIVE);
        Matcher matcher = protocol.matcher(url);
        String resProtocol = null;
        String resDomain = null;
        String referPath = null;
        int portNumber = -1;

        // get protocol and domain name from url, otherwise from refer url
        if(matcher.find()){
            if(httpProtocol.matcher(url).find()){
                resProtocol = "http";
            }else{
                resProtocol = "https";
            }
            url = url.substring(matcher.end());
            if(url.indexOf('/')!=-1){
                resDomain = url.substring(0, url.indexOf('/'));
                url = url.substring(url.indexOf('/'));
            }else{
                resDomain = url;
                url = "";
            }
        }else if(Pattern.compile("://", Pattern.CASE_INSENSITIVE).matcher(url).find()) { throw new Exception("Unknown protocol");
        }else if (referNormalized==null){
            throw new Exception("Cannot normalize url!");
        }
        else if(httpProtocol.matcher(referNormalized).find()){
            resProtocol = "http";
            resDomain = referNormalized.substring(7);
            resDomain = resDomain.substring(0, resDomain.indexOf('/'));
        }else {
            resProtocol = "https";
            resDomain = referNormalized.substring(8);
            resDomain = resDomain.substring(0, resDomain.indexOf('/'));
        }

        if(referNormalized!=null){
            // get refer path
            if(httpProtocol.matcher(referNormalized).find()){
                referPath = referNormalized.substring(7);
            }else {
                referPath = referNormalized.substring(8);
            }
            referPath = referPath.substring(referPath.indexOf('/'));
        }

        // extract port number
        if(resDomain.split(":").length>1){
            portNumber = Integer.parseInt(resDomain.split(":")[1]);
            resDomain = resDomain.substring(0, resDomain.indexOf(":"));
        }else{
            portNumber = resProtocol.equals("http")? 80 : 443;
        }

        // delete element ref
        if(url.indexOf('#')!=-1){
            url = url.substring(0, url.indexOf('#'));
        }
        if(!url.equals("")){
            if(referPath!=null && referPath.lastIndexOf('/')!=-1 && referPath.substring(referPath.lastIndexOf('/')).indexOf('.')!=-1){
                referPath = referPath.substring(0, referPath.lastIndexOf("/"));
            }
            if(url.charAt(0)=='.' || url.indexOf('/')==-1) {
                url = referPath + "/" + url;
            }
        }else if (referPath!=null){
            url = referPath;
        }
        if(!url.equals("") && url.charAt(0)=='/'){
            url = url.substring(1);
        }
        LinkedList<String> urlPath = new LinkedList<>();
        for(String s : url.split("/")){
            if(s.equals("..")){
                urlPath.pop();
                continue;
            }
            urlPath.push(s);
        }
        url = "";
        for(String s: urlPath){
            url = "/"+ s +url;
        }
        if(url.lastIndexOf(".")!=-1 && !url.substring(url.lastIndexOf(".")+1).equals("html")) throw new Exception("Invalid file type: "+url);
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append(resProtocol);
        urlBuilder.append("://");
        urlBuilder.append(resDomain);
        urlBuilder.append(":");
        urlBuilder.append(portNumber);
        urlBuilder.append(url);
        return urlBuilder.toString();
    }
    public static List<String> parseHtml(String html) throws IOException {
        StringBuilder tagBuilder = new StringBuilder();
        LinkedList<String> hrefs = new LinkedList<>();
        boolean inTag = false;
        boolean inString = false;
        for(int j=0;j<html.length();j++){
            int i = html.charAt(j);
            if(!inString && !inTag && i=='<'){
                inTag = true;
            }
            if(inTag){
                tagBuilder.append((char)i);
            }
            if(inTag && i=='"'){
                inString = !inString;
            }
            if(!inString && inTag && i=='>'){
                inTag = false;
                String tagString = tagBuilder.toString();
                String href = getUrlFromATag(tagString);
                if(href!=null){
                    hrefs.add(href);
                }
                tagBuilder = new StringBuilder();
            }
        }
        return hrefs;
    }
    public static void run(FlameContext flameContext, String[] seedUrls) throws Exception {
        FlamePairRDD flamePairRDD = flameContext.fromTable("pt-crawl", (Row row)->{
            try {
                StringBuilder hrefs = new StringBuilder();
                for(String url : parseHtml(row.get("page"))) {
                    url = normalizeUrl(url, row.get("url"));
                    hrefs.append(",").append(Hasher.hash(url));
                }
                return Hasher.hash(row.get("url")) + "|" + "1.0,1.0"+hrefs;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).mapToPair((String s)->{
            return new FlamePair(s.split("\\|")[0],s.split("\\|")[1]);
        });
        flamePairRDD.saveAsTable("state_table");
        double maxDifference = 0.0;
        do {
            maxDifference = 0.0;
            FlamePairRDD flamePairRDD1 = flamePairRDD.flatMapToPair((FlamePair pair) -> {
                String[] strings = pair._2().split(",");
                double rc = Double.parseDouble(strings[0]);
                LinkedList<FlamePair> pairList = new LinkedList<>();
                pairList.add(new FlamePair(pair._1(), String.valueOf(0.0)));
                if(strings.length==2){
                    return pairList;
                }
                int i = 2;
                while(i<strings.length){
                    pairList.add(new FlamePair(strings[i],  String.valueOf(0.85*rc / (strings.length-2))));
                    i++;
                }
                return pairList;
            }).foldByKey("0.0", (String s1, String s2)->{
                double v1 = Double.parseDouble(s1);
                double v2 = Double.parseDouble(s2);
                return String.valueOf(v1 + v2);
            });
            flamePairRDD1.saveAsTable("transfer_table");
            flamePairRDD = flamePairRDD.join(flamePairRDD1).flatMapToPair((FlamePair pair)->{
                try{
                    String[] strings = pair._2().split(",");
                    LinkedList<String> linkedList = new LinkedList<>();
                    double rp = Double.parseDouble(strings[0]);
                    double rc = Double.parseDouble(strings[strings.length-1])+0.15;
                    linkedList.add(rc +",");
                    linkedList.add(String.valueOf(rp));
                    if(strings.length>3){
                        linkedList.add(",");
                        for(int i = 2; i<strings.length-1;i++){
                            if(i!=strings.length-2){
                                linkedList.add(strings[i]+",");
                            }else{
                                linkedList.add(strings[i]);
                            }
                        }
                    }
                    StringBuilder stringBuilder = new StringBuilder();
                    for(String s : linkedList){
                        stringBuilder.append(s);
                    }
                    System.out.println(stringBuilder);
                    LinkedList<FlamePair> list = new LinkedList<>();
                    list.add(new FlamePair(pair._1(), stringBuilder.toString()));
                    return list;
                }catch (Exception e){
                    e.printStackTrace();
                }
                return new LinkedList<>();
            });
            String foldRes = flamePairRDD.flatMap((FlamePair pair)->{
                String s = String.valueOf(
                        Math.abs(
                                Double.parseDouble(pair._2().split(",")[0])
                                        - Double.parseDouble(pair._2().split(",")[1])
                        )
                );
                LinkedList<String> list = new LinkedList<>();
                list.add(s);
                return list;
            }).fold(String.valueOf(maxDifference), (String s1, String s2) ->{
                return String.valueOf(Math.max(Double.parseDouble(s1),Double.parseDouble(s2)));
            });
            maxDifference = Double.parseDouble(foldRes);
        }while(maxDifference>0.01);
        flamePairRDD.flatMapToPair((FlamePair pair)->{
            double rank = Double.parseDouble(pair._2().split(",")[0]);
            flameContext.getKVS().put("pt-pageranks", pair._1(), "rank", String.valueOf(rank));
            return new LinkedList<>();
        });
    }
}
