package dev.krabka.sdk;

public final class TransportException extends KrabkaException {
    public TransportException(String message) {
        super(message);
    }

    public TransportException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String kind() {
        return "transport";
    }
}
