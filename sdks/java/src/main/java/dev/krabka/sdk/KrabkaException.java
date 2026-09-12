package dev.krabka.sdk;

public abstract class KrabkaException extends RuntimeException {
    protected KrabkaException(String message) {
        super(message);
    }

    protected KrabkaException(String message, Throwable cause) {
        super(message, cause);
    }

    public abstract String kind();
}
