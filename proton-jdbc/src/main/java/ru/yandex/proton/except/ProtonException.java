package ru.yandex.proton.except;

import java.sql.SQLException;

public class ProtonException extends SQLException {

    public ProtonException(int code, Throwable cause, String host, int port) {
        super("Proton exception, code: " + code + ", host: " + host + ", port: " + port + "; "
                + (cause == null ? "" : cause.getMessage()), null, code, cause);
    }

    public ProtonException(int code, String message, Throwable cause, String host, int port) {
        super("Proton exception, message: " + message + ", host: " + host + ", port: " + port + "; "
                + (cause == null ? "" : cause.getMessage()), null, code, cause);
    }

    public ProtonException(int code, String message, Throwable cause) {
        super("Proton exception, message: " + message + "; "
                + (cause == null ? "" : cause.getMessage()), null, code, cause);
    }

}
