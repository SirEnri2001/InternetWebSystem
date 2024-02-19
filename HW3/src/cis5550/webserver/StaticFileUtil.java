package cis5550.webserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class StaticFileUtil {
    public static void writeStaticFile(String filename, byte[] content, boolean append) throws IOException {
        File file = new File(Server.getLocation() +"/"+ filename);
        file.setWritable(true);
        FileOutputStream fileOutputStream = new FileOutputStream(file, append);
        fileOutputStream.write(content);
    }

    public static byte[] getStaticFile(String filename, int offset, int length) throws IOException {
        File file = new File(filename);
        FileInputStream fileInputStream = new FileInputStream(file);
        fileInputStream.readNBytes(offset);
        if(length<0){
            return fileInputStream.readAllBytes();
        }
        return fileInputStream.readNBytes(length);
    }
}
