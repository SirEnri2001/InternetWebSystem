package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

import javax.naming.Context;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
            if(j+2<strings.length && strings[j].compareToIgnoreCase("href")==0){
                href = strings[j+2];
                break;
            }
        }
        return href;
    }

    private static String getHostName(String normalizedUrl) {
        Pattern protocol = Pattern.compile("https?://", Pattern.CASE_INSENSITIVE);
        Matcher matcher = protocol.matcher(normalizedUrl);
        if(matcher.find()){
            normalizedUrl = normalizedUrl.substring(matcher.end());
        }
        normalizedUrl = normalizedUrl.substring(0, normalizedUrl.indexOf(':'));
        return normalizedUrl;
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
    public static void run(FlameContext flameContext, String[] seedUrls) {
        try{
            String tableName = "pt-crawl";
            String hostsTableName = "hosts";
            if(seedUrls.length==0){
                flameContext.output("No element found!");
                return;
            }
            LinkedList<String> list = new LinkedList<String>();
            int[] redirectCodes = {301, 302, 303, 307, 308};
            for (int i=0; i<seedUrls.length; i++)
                list.add(normalizeUrl(seedUrls[i], null));
            FlameRDD.StringToIterable lambdaFunction = (String urlString)->{
                try{
                    if(flameContext.getKVS().getRow(tableName, Hasher.hash(urlString))!=null){
                        return new LinkedList<>();
                    }
                    URL url = new URL(urlString);
                    String hostName= getHostName(urlString);
                    Row hostRow = flameContext.getKVS().getRow(hostsTableName, Hasher.hash(hostName));
                    if(hostRow!=null && System.currentTimeMillis() - Long.parseLong(hostRow.get("lastAccessed"))<1000L){
                        LinkedList<String> ret = new LinkedList<>();
                        ret.add(urlString);
                        Thread.sleep(1000);
                        return ret;
                    }
                    flameContext.getKVS().put(hostsTableName,Hasher.hash(hostName),"lastAccessed", String.valueOf(System.currentTimeMillis()));
                    HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
                    httpURLConnection.setRequestMethod("HEAD");
                    httpURLConnection.setRequestProperty("User-Agent","cis5550-crawler");
                    Row row = new Row(Hasher.hash(urlString));
                    row.put("url", urlString);
                    row.put("responseCode", String.valueOf(httpURLConnection.getResponseCode()));
                    if(httpURLConnection.getResponseCode()!=200){
                        for(int code : redirectCodes){
                            if(code== httpURLConnection.getResponseCode())  {
                                String newUrl = httpURLConnection.getHeaderField("Location");
                                flameContext.getKVS().putRow(tableName, row);
                                LinkedList<String> ret = new LinkedList<>();
                                ret.add(newUrl);
                                return ret;
                            }
                        }
                        flameContext.getKVS().putRow(tableName, row);
                        return new LinkedList<>();
                    }
                    row.put("contentType", httpURLConnection.getContentType());
                    row.put("length", String.valueOf(httpURLConnection.getContentLength()));

                    if(!httpURLConnection.getContentType().contains("text/html")){
                        flameContext.getKVS().putRow(tableName, row);
                        return new LinkedList<>();
                    }
                    httpURLConnection = (HttpURLConnection) url.openConnection();
                    httpURLConnection.setRequestMethod("GET");
                    httpURLConnection.setRequestProperty("User-Agent","cis5550-crawler");
                    if(httpURLConnection.getResponseCode()!=200){
                        flameContext.getKVS().putRow(tableName, row);
                        return new LinkedList<>();
                    }
                    StringBuilder html = new StringBuilder();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(httpURLConnection.getInputStream()));
                    while(html.isEmpty() || bufferedReader.ready()){
                        html.append((char)bufferedReader.read());
                    }
                    row.put("page", html.toString());
                    List<String> hrefs = parseHtml(html.toString());
                    LinkedList<String> normalizedHrefs = new LinkedList<>();
                    for(String s : hrefs){
                        normalizedHrefs.add(normalizeUrl(s, urlString));
                    }
                    flameContext.getKVS().putRow(tableName, row);
                    return normalizedHrefs;
                }catch (Exception e){
                }
                return new LinkedList<>();
            };
            urlQueue = flameContext.parallelize(list);
            while(urlQueue.count()!=0) {
                urlQueue = urlQueue.flatMap(lambdaFunction);
            }
            flameContext.output("OK");
            return;
        }catch (Exception e){
            flameContext.output(e.toString());
            for(StackTraceElement element : e.getStackTrace()){
                flameContext.output(element.toString());
                flameContext.output("\n");
            }
        }
    }

    public static void main(String[] strings) throws Exception {
        System.out.println(normalizeUrl("../../a/","https://foo.com:8000/bar/foo/xyz.html"));
    }
}
