package io.pravega.shared.health.bindings.api;

@SuppressWarnings("all")
public class ApiException extends Exception{
    private int code;
    public ApiException(int code, String msg) {
        super(msg);
        this.code = code;
    }
}
