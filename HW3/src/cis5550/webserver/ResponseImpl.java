package cis5550.webserver;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.text.Format;
import java.util.Arrays;

public class ResponseImpl implements Response {
    byte[] body;
    String contentType;
    StringBuilder headerBuilder = new StringBuilder();
    int statusCode;
    String statusDetail;
    public boolean isHeaderWritten;
    Socket socket;
    public ResponseImpl(Socket socket){
        this.socket = socket;
    }
    @Override
    public void body(String body) {
        bodyAsBytes(body.getBytes());
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        body = bodyArg;
    }

    @Override
    public void header(String name, String value) {
        headerBuilder.append(name);
        headerBuilder.append(":");
        headerBuilder.append(value);
        headerBuilder.append("\r\n");
    }

    @Override
    public void type(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        this.statusCode = statusCode;
        this.statusDetail = reasonPhrase;
    }

    @Override
    public void write(byte[] b) throws Exception {
        PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
        if(!isHeaderWritten){
            isHeaderWritten = true;
            printWriter.write(String.format("HTTP/1.1 %1$d %2$s\r\n", statusCode, statusDetail));
            if(contentType!=null){
                printWriter.write(String.format("Content-Type: %1$s\r\n",contentType));
            }
            printWriter.write(headerBuilder.toString());
            printWriter.write("\r\n");
        }
        printWriter.flush();
        socket.getOutputStream().write(b);
        socket.getOutputStream().flush();
    }

    @Override
    public void redirect(String url, int responseCode) {

    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {

    }
}
