package com.timeplus.proton.except;

import org.apache.http.conn.ConnectTimeoutException;

import com.timeplus.proton.util.Utils;

import java.net.ConnectException;
import java.net.SocketTimeoutException;

import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;

/**
 * Specify Proton exception to ProtonException and fill it with a vendor
 * code.
 */

public final class ProtonExceptionSpecifier {

    private static final Logger log = LoggerFactory.getLogger(ProtonExceptionSpecifier.class);

    private ProtonExceptionSpecifier() {
    }

    public static ProtonException specify(Throwable cause, String host, int port) {
        return specify(cause != null ? cause.getMessage() : null, cause, host, port);
    }

    public static ProtonException specify(String protonMessage, String host, int port) {
        return specify(protonMessage, null, host, port);
    }

    public static ProtonException specify(String protonMessage) {
        return specify(protonMessage, "unknown", -1);
    }

    /**
     * Here we expect the Proton error message to be of the following format:
     * "Code: 10, e.displayText() = DB::Exception: ...".
     */
    private static ProtonException specify(String protonMessage, Throwable cause, String host, int port) {
        if (Utils.isNullOrEmptyString(protonMessage) && cause != null) {
            return getException(cause, host, port);
        }

        try {
            int code;
            if (protonMessage.startsWith("Poco::Exception. Code: 1000, ")) {
                code = 1000;
            } else {
                // Code: 175, e.displayText() = DB::Exception:
                code = getErrorCode(protonMessage);
            }
            // ошибку в изначальном виде все-таки укажем
            Throwable messageHolder = cause != null ? cause : new Throwable(protonMessage);
            if (code == -1) {
                return getException(messageHolder, host, port);
            }

            return new ProtonException(code, messageHolder, host, port);
        } catch (Exception e) {
            log.error(
                    "Unsupported Proton error format, please fix ProtonExceptionSpecifier, message: %s, error: %s",
                    protonMessage, e.getMessage());
            return new ProtonUnknownException(protonMessage, cause, host, port);
        }
    }

    private static int getErrorCode(String errorMessage) {
        int startIndex = errorMessage.indexOf(' ');
        if (startIndex >= 0) {
            for (int i = ++startIndex, len = errorMessage.length(); i < len; i++) {
                char ch = errorMessage.charAt(i);
                if (ch == '.' || ch == ',' || Character.isWhitespace(ch)) {
                    try {
                        return Integer.parseInt(errorMessage.substring(startIndex, i));
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                    break;
                }
            }
        }

        return -1;
    }

    private static ProtonException getException(Throwable cause, String host, int port) {
        if (cause instanceof SocketTimeoutException)
        // if we've got SocketTimeoutException, we'll say that the query is not good.
        // This is not the same as SOCKET_TIMEOUT of proton
        // but it actually could be a failing Proton
        {
            return new ProtonException(ProtonErrorCode.TIMEOUT_EXCEEDED.code, cause, host, port);
        } else if (cause instanceof ConnectTimeoutException || cause instanceof ConnectException)
        // couldn't connect to Proton during connectTimeout
        {
            return new ProtonException(ProtonErrorCode.NETWORK_ERROR.code, cause, host, port);
        } else {
            return new ProtonUnknownException(cause, host, port);
        }
    }

}
