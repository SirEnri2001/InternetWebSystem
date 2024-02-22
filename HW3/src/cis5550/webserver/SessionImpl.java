package cis5550.webserver;

import java.util.concurrent.ConcurrentHashMap;

public class SessionImpl implements Session{
    String sessionId;
    long lastAccessTime;
    long createTime;
    long maxTimeInterval=300000;
    ConcurrentHashMap<String, Object> values = new ConcurrentHashMap<>();
    Server server;
    @Override
    public String id() {
        return sessionId;
    }

    @Override
    public long creationTime() {
        return this.createTime;
    }

    @Override
    public long lastAccessedTime() {
        return this.lastAccessTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        maxTimeInterval = seconds*1000L;
    }

    @Override
    public void invalidate() {

        server.putSession(sessionId, null);
    }

    @Override
    public Object attribute(String name) {
        return values.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        values.put(name,value);
    }

    SessionImpl(Server server){
        this.server = server;
    }

    public void updateLastAccessTime() {
        lastAccessTime = System.currentTimeMillis();
    }
}
