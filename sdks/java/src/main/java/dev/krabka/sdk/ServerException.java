package dev.krabka.sdk;

public final class ServerException extends KrabkaException {
    public ServerException(String message) {
        super(message);
    }

    @Override
    public String kind() {
        return "server_error";
    }
}
