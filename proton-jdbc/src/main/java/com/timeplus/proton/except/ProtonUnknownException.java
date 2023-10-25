package com.timeplus.proton.except;

public class ProtonUnknownException extends ProtonException {

    public ProtonUnknownException(Throwable cause, String host, int port) {
        super(ProtonErrorCode.UNKNOWN_EXCEPTION.code, cause, host, port);
    }


    public ProtonUnknownException(String message, Throwable cause, String host, int port) {
        super(ProtonErrorCode.UNKNOWN_EXCEPTION.code, message, cause, host, port);
    }

    public ProtonUnknownException(String message, Throwable cause) {
        super(ProtonErrorCode.UNKNOWN_EXCEPTION.code, message, cause);
    }

    public ProtonUnknownException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }

}
