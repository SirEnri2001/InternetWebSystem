package cis5550.test;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.io.IOException;
import java.util.Iterator;

public class MyTest {
    public static void main(String[] args) throws IOException {
        KVSClient kvsClient = new KVSClient("localhost:8000");
        Iterator<Row> res = kvsClient.scan("5d5dc52c-4", "0a", "de");
        for (Iterator<Row> it = res; it.hasNext(); ) {
            Row row = it.next();
            System.out.println(row.get("value"));

        }
    }
}
