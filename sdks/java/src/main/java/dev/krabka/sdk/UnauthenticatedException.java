package dev.krabka.sdk;

public final class UnauthenticatedException extends KrabkaException {
    public UnauthenticatedException(String message) {
        super(message);
    }

    @Override
    public String kind() {
        return "unauthenticated";
    }
}
