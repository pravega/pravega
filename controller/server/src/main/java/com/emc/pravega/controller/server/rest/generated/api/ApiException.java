package com.emc.pravega.controller.server.rest.generated.api;


public class ApiException extends Exception{
    private int code;
    public ApiException (int code, String msg) {
        super(msg);
        this.code = code;
    }
}
