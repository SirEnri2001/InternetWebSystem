package cis5550.webserver;


import cis5550.tools.Logger;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

class RequestHandler implements Callable<Object> {
    Socket socket;
    private final Logger logger;
    private final ConcurrentHashMap<String[], Route> getMap;
    private final ConcurrentHashMap<String[], Route> putMap;
    private final ConcurrentHashMap<String[], Route> postMap;
    public RequestHandler(Socket socket, Logger logger,
                          ConcurrentHashMap<String[], Route> getMap,
                          ConcurrentHashMap<String[], Route> putMap,
                          ConcurrentHashMap<String[], Route> postMap
    ){
        this.socket = socket;
        this.logger = logger;
        this.getMap = getMap;
        this.putMap = putMap;
        this.postMap = postMap;
    }

    public Route matchRoute(String uri, Request request, final ConcurrentHashMap<String[], Route> map) {
        while(uri.startsWith("/")){
            uri = uri.substring(1);
        }
        String[] uriPaths = uri.split("/+");
        for (Map.Entry<String[], Route> entry : map.entrySet()) {
            String[] entryPath = entry.getKey();
            if(entryPath.length!=uriPaths.length){
                continue;
            }
            boolean matched = true;
            for(int i = 0;i<entryPath.length;i++){
                if(entryPath[i].startsWith(":")){
                    request.params().put(entryPath[i].substring(1),uriPaths[i]);
                    continue;
                }
                if(entryPath[i].equals(uriPaths[i])){
                    continue;
                }
                matched = false;
                break;
            }
            if(!matched){
                continue;
            }
            return entry.getValue();
        }
        return null;
    }

    public void appendQueryMap(String queryString, Map<String, String> mapToAppend) throws HttpException{
        if(queryString==null || queryString.isBlank()){
            return;
        }
        try{
            String[] encodedQueryStrings = queryString.split("&");
            for(String encoded : encodedQueryStrings){
                String[] nameVal = encoded.split("=");
                String name = java.net.URLDecoder.decode(nameVal[0], StandardCharsets.UTF_8);
                String val = java.net.URLDecoder.decode(nameVal[1], StandardCharsets.UTF_8);
                mapToAppend.put(name, val);
            }
        }catch (Exception e){
            throw new HttpException(400, "Query Params Error");
        }

    }

    @Override
    public Object call() throws Exception {
        doRequestWorker();
        return null;
    }

    public void onError(Socket socket, HttpException httpException) throws IOException {
        PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
        printWriter.println(
                String.format("HTTP/1.1 %1$s %2$s\r\n\r\n",
                        httpException.errCode,
                        httpException.description));
        printWriter.flush();
    }

    public void onStaticGet(HashMap<String, String> headerMap) throws HttpException, IOException {
        Path file = null;
        BasicFileAttributes attr = null;

        try{
            file = Paths.get(Server.getLocation(), headerMap.get("uri"));
            attr = Files.readAttributes(file, BasicFileAttributes.class);
        }catch (InvalidPathException ipe){
            throw new HttpException(400, "Invalid Path");
        }catch (IOException ioe){
            logger.error("File "+file+" not found");
            throw new HttpException(404, "File not found");
        }

        PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
        FileTime fileTime = null;
        if(headerMap.get("If-Modified-Since".toLowerCase())!=null && headerMap.get("uri")!=null){
            fileTime = attr.lastModifiedTime();
            DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;
            TemporalAccessor ret = null;
            try{
                ret = formatter.parse(headerMap.get("If-Modified-Since".toLowerCase()));
            }catch(DateTimeParseException e){
                throw new HttpException(400, "Bad request: Datetime in If-Modified-Since is Invalid");
            }
            try{
                long requestedSince = (ret.getLong(ChronoField.EPOCH_DAY) *24*60*60+ret.getLong(ChronoField.OFFSET_SECONDS))*1000;
                long fileModifiedSince = fileTime.toMillis();
                if(requestedSince>fileModifiedSince){
                    printWriter.write("HTTP/1.1 304 Not Modified\r\n\r\n");
                    printWriter.flush();
                    return;
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        try{
            printWriter.write("HTTP/1.1 200 OK\r\n");
            byte[] fileContent = StaticFileUtil.getStaticFile(
                    file.toString(),
                    0, -1
            );
            Files.setAttribute(file, "basic:lastModifiedTime", fileTime);
            printWriter.write(String.format("Content-Length: %1$s\r\n\r\n",fileContent.length));
            printWriter.flush();
            socket.getOutputStream().write(fileContent);
        }catch (FileNotFoundException fnf){
            throw new HttpException(404, "Not Found");
        }
        printWriter.flush();
        logger.info(String.format("Remote %1$s : %2$s %3$s",
                socket.getRemoteSocketAddress(), 200, "OK"));
    }
    public void onStaticPut(HashMap<String, String> headerMap, byte[] body) throws IOException {
        StaticFileUtil.writeStaticFile(Paths.get(Server.getLocation(), headerMap.get("uri")).toString(), body, false);
        PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
        printWriter.write("HTTP/1.1 200 OK\r\n");
        printWriter.flush();
        logger.info(String.format("Remote %1$s : %2$s %3$s",
                socket.getRemoteSocketAddress(), 200, "OK"));

    }
    public void onStaticPost(HashMap<String, String> headerMap, byte[] body) throws IOException {
        StaticFileUtil.writeStaticFile(Paths.get(Server.getLocation(), headerMap.get("uri")).toString(), body, true);
        PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
        printWriter.write("HTTP/1.1 200 OK\r\n");
        printWriter.flush();
        logger.info(String.format("Remote %1$s : %2$s %3$s",
                socket.getRemoteSocketAddress(), 200, "OK"));
    }

    public void onRouting(Route r, Request req, Response resp) throws HttpException, IOException {
        resp.status(200, "OK");
        Object obj;
        try{
            obj = r.handle(req, resp);
            logger.info("Handle exits");
        }catch (Exception e){
            if(((ResponseImpl)resp).isHeaderWritten){
                throw new SocketException("Server Error, close connection");
            }
            throw new HttpException(500, "Internal Server Error on "+getMap.get("Method")+" "+getMap.get("Uri"));
        }
        try{
            if(obj!=null){
                if(obj instanceof String){
                    String str = (String)obj;
                    resp.type("text/plain");
                    resp.header("Content-Length", String.valueOf(str.length()));
                    resp.write(str.getBytes());
                }else if(obj instanceof byte[]){
                    byte[] bytes = (byte[]) obj;
                    resp.type("application/octet-stream");
                    resp.header("Content-Length", String.valueOf(bytes.length));
                    resp.write(bytes);
                }
            }else{
                socket.close();
            }
        }catch (Exception e){
            throw new SocketException("Server Error, close connection");
        }

    }

    public void acceptIncoming() throws IOException, HttpException{
        boolean isEOF = false;
        while(!isEOF){
            socket.getOutputStream().flush();
            logger.info("Incoming connection from: "+socket.getRemoteSocketAddress());
            HashMap<String, String> headerMap = getHeaderMap(socket);
            switch(headerMap.get("method").toUpperCase()){
                case "GET":
                case "POST":
                case "PUT":
                    break;
                case "HEAD":
                case "DELETE":
                case "CONNECT":
                case "OPTION":
                case "TRACE":
                case "PATCH":
                    throw new HttpException(405, "Method Not Allowed");
                default:
                    throw new HttpException(501, "Not Implemented");
            }
            if(!headerMap.get("version").equalsIgnoreCase("HTTP/1.1")){
                throw new HttpException(505, "Version Not Supported");
            }
            validateHeaderMap(headerMap);
            if(headerMap.get("connection")!=null && headerMap.get("connection").equalsIgnoreCase("close")){
                isEOF = true;
            }
            byte[] reqBody = null;
            if(headerMap.get("content-length")!=null){
                reqBody = getBodyString(socket, Integer.parseInt(headerMap.get("content-length")));
            }
            Route r = null;
            InetSocketAddress socketAddress = new InetSocketAddress(socket.getInetAddress(),socket.getPort());
            HashMap<String, String> queryMap = new HashMap<>();
            appendQueryMap(headerMap.get("query"), queryMap);
            if(headerMap.get("content-type")!=null && headerMap.get("content-type").equalsIgnoreCase("application/x-www-form-urlencoded")){
                if(reqBody==null){
                    throw new HttpException(400, "Bad request");
                }
                appendQueryMap(new String(reqBody), queryMap);
            }
            Request req = new RequestImpl(
                    headerMap.get("method"),
                    headerMap.get("uri"),
                    headerMap.get("version"),
                    headerMap,
                    queryMap,
                    new HashMap<>(),
                    socketAddress,
                    reqBody,
                    Server.serverInstance
            );
            Response resp = new ResponseImpl(socket);

            switch(headerMap.get("method").toUpperCase()){
                case "GET":
                    r = matchRoute(headerMap.get("uri"), req, getMap);
                    if(r==null) {
                        onStaticGet(headerMap);
                    }
                    break;
                case "PUT":
                    r = matchRoute(headerMap.get("uri"), req, putMap);
                    if(r==null) {
                        onStaticPut(headerMap, reqBody);
                    }
                    break;
                case "POST":
                    r = matchRoute(headerMap.get("uri"), req, postMap);
                    if(r==null) {
                        onStaticPost(headerMap, reqBody);
                    }
                    break;
                default:
                    throw new HttpException(405, "Method Not Allowed in Static Files");
            }
            if(r!=null){
                onRouting(r, req, resp);
            }
        }
    }

    public String readLine(Socket socket) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        while(true){
            int readRes = socket.getInputStream().read();
            if (readRes==-1){
                throw new SocketException("Socket Closed On Remote");
            }
            stringBuilder.append((char)readRes);
            if(stringBuilder.charAt(stringBuilder.length()-1)=='\n'){
                logger.info("Socket read: " + stringBuilder.toString().trim());
                return stringBuilder.toString().trim();
            }
        }
    }

    public LinkedList<String> getHeaderStringList(Socket socket) throws IOException {
        LinkedList<String> headerStringArray = new LinkedList<>();
        String line = readLine(socket);
        while(!line.isEmpty()){
            headerStringArray.add(line);
            line = readLine(socket);
        }
        return headerStringArray;
    }

    public HashMap<String, String> getHeaderMap(Socket socket) throws HttpException, IOException {
        LinkedList<String> headerStringList = getHeaderStringList(socket);
        HashMap<String, String> retMap = new HashMap<>();
        boolean isMethodLine = true;
        for(String line : headerStringList){
            if(isMethodLine){
                try{
                    String[] methodUriVersion = line.split(" ");
                    retMap.put("method", methodUriVersion[0].trim().toLowerCase());
                    String uriWithQuery = methodUriVersion[1].trim();
                    if(uriWithQuery.contains("?")){
                        String[] uriQuery = uriWithQuery.split("\\\\*\\?");
                        retMap.put("uri", uriQuery[0]);
                        retMap.put("query", uriQuery[1]);
                    }else{
                        retMap.put("uri", uriWithQuery);
                    }
                    retMap.put("version", methodUriVersion[2].trim().toLowerCase());
                    if(line.indexOf(':')!=-1){
                        throw new HttpException(400, "Bad Request");
                    }
                }catch (ArrayIndexOutOfBoundsException e){
                    throw new HttpException(400, "Bad Request");
                }
                isMethodLine = false;
                continue;
            }
            try{
                int separate = line.indexOf(":");
                String headerItem = line.substring(0,separate).trim();
                String headerValue = line.substring(separate+1).trim();
                retMap.put(headerItem.toLowerCase(), headerValue);
            }catch (ArrayIndexOutOfBoundsException e){
                throw new HttpException(400, "Bad Request");
            }
        }
        return retMap;
    }

    public byte[] getBodyString(Socket socket, int contentLength) throws SocketException, IOException {
        byte[] resBody = new byte[contentLength];
        for(int i = 0;i<contentLength;i++){
            resBody[i] = (byte) socket.getInputStream().read();
        }
        return resBody;
    }

    public void validateHeaderMap(HashMap<String, String> headerMap) throws HttpException {
        String uri = headerMap.get("uri");
        uri = uri.replace('\\','/');
        StringBuilder uriBuilder = new StringBuilder();
        ArrayList<String> uris = new ArrayList<>();
        for(String s : uri.split("/")){
            if(s.isEmpty()){
                continue;
            }
            if(s.equals("..")){
                if(uris.isEmpty()){
                    continue;
                }
                uris.remove(uris.size()-1);
            }
            uriBuilder.append(s);
            uriBuilder.append("/");
        }
        for(String s : uris){
            uriBuilder.append(s);
        }
        headerMap.put("uri", uriBuilder.toString());
    }

    public void doRequestWorker() throws IOException{
        try{
            try{
                acceptIncoming();
            } catch (HttpException httpe){
                logger.info(String.format("Remote %1$s : %2$s %3$s",
                        socket.getRemoteSocketAddress(), httpe.errCode, httpe.description));
                onError(socket, httpe);
                httpe.printStackTrace();
            }
            socket.close();
        }catch (SocketException socketException){
            logger.info(String.format("Remote %1$s : %2$s",
                    socket.getRemoteSocketAddress(), socketException.getMessage()));
        }
        socket.close();
    }
}