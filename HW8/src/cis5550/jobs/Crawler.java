package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.net.HttpURLConnection;
import java.net.URL;

public class Crawler {
    static FlameRDD urlQueue;
    public static void run(FlameContext flameContext, String[] seedUrls) throws Exception {
        if(seedUrls.length==0){
            flameContext.output("No element found!");
            return;
        }
        for(String url : seedUrls) {

        }
        while(urlQueue.count()!=0){
            urlQueue = urlQueue.flatMap((String urlString)->{
                URL url = new URL(urlString);
                HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
                httpURLConnection.setRequestMethod("GET");
                httpURLConnection.setRequestProperty("","");
                httpURLConnection.connect();

            });
        }
        flameContext.output("OK");
        return;
    }



}
