package io.pravega.shared.health.bindings.api;

@SuppressWarnings("all")
public class NotFoundException extends ApiException {
    private int code;
    public NotFoundException (int code, String msg) {
        super(code, msg);
        this.code = code;
    }
}
