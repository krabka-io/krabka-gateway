package dev.krabka.sdk;

public final class InvalidArgumentException extends KrabkaException {
    public InvalidArgumentException(String message) {
        super(message);
    }

    @Override
    public String kind() {
        return "invalid_argument";
    }
}
