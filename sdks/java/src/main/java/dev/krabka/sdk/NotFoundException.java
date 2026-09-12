package dev.krabka.sdk;

public final class NotFoundException extends KrabkaException {
    public NotFoundException(String message) {
        super(message);
    }

    @Override
    public String kind() {
        return "not_found";
    }
}
