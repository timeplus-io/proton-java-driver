package com.timeplus.proton.client.logging;

import java.util.ResourceBundle;
import java.util.function.Supplier;
import java.util.logging.Level;
import com.timeplus.proton.client.ProtonChecker;

/**
 * Adaptor for JDK logger.
 */
public class JdkLogger implements Logger {
    private final java.util.logging.Logger logger;

    protected void log(Level level, LogMessage msg) {
        if (msg.hasThrowable()) {
            logger.logrb(level, (String) null, (String) null, (ResourceBundle) null, msg.getMessage(),
                    msg.getThrowable());
        } else {
            logger.logrb(level, (String) null, (String) null, (ResourceBundle) null, msg.getMessage());
        }
    }

    /**
     * Default constructor.
     *
     * @param logger non-null JDK logger
     */
    public JdkLogger(java.util.logging.Logger logger) {
        this.logger = ProtonChecker.nonNull(logger, "logger");
    }

    @Override
    public void debug(Supplier<?> function) {
        if (function != null && logger.isLoggable(Level.FINE)) {
            log(Level.FINE, LogMessage.of(function.get()));
        }
    }

    @Override
    public void debug(Object format, Object... arguments) {
        if (logger.isLoggable(Level.FINE)) {
            log(Level.FINE, LogMessage.of(format, arguments));
        }
    }

    @Override
    public void debug(Object message, Throwable t) {
        if (logger.isLoggable(Level.FINE)) {
            logger.logrb(Level.FINE, (String) null, (String) null, (ResourceBundle) null, String.valueOf(message), t);
        }
    }

    @Override
    public void error(Supplier<?> function) {
        if (function != null && logger.isLoggable(Level.SEVERE)) {
            log(Level.SEVERE, LogMessage.of(function.get()));
        }
    }

    @Override
    public void error(Object format, Object... arguments) {
        if (logger.isLoggable(Level.SEVERE)) {
            log(Level.SEVERE, LogMessage.of(format, arguments));
        }
    }

    @Override
    public void error(Object message, Throwable t) {
        if (logger.isLoggable(Level.SEVERE)) {
            logger.logrb(Level.SEVERE, (String) null, (String) null, (ResourceBundle) null, String.valueOf(message), t);
        }
    }

    @Override
    public void info(Supplier<?> function) {
        if (function != null && logger.isLoggable(Level.INFO)) {
            log(Level.INFO, LogMessage.of(function.get()));
        }
    }

    @Override
    public void info(Object format, Object... arguments) {
        if (logger.isLoggable(Level.INFO)) {
            log(Level.INFO, LogMessage.of(format, arguments));
        }
    }

    @Override
    public void info(Object message, Throwable t) {
        if (logger.isLoggable(Level.INFO)) {
            logger.logrb(Level.INFO, (String) null, (String) null, (ResourceBundle) null, String.valueOf(message), t);
        }
    }

    @Override
    public void trace(Supplier<?> function) {
        if (function != null && logger.isLoggable(Level.FINEST)) {
            log(Level.FINEST, LogMessage.of(function.get()));
        }
    }

    @Override
    public void trace(Object format, Object... arguments) {
        if (logger.isLoggable(Level.FINEST)) {
            log(Level.FINEST, LogMessage.of(format, arguments));
        }
    }

    @Override
    public void trace(Object message, Throwable t) {
        if (logger.isLoggable(Level.FINEST)) {
            logger.logrb(Level.FINEST, (String) null, (String) null, (ResourceBundle) null, String.valueOf(message), t);
        }
    }

    @Override
    public void warn(Supplier<?> function) {
        if (function != null && logger.isLoggable(Level.WARNING)) {
            log(Level.WARNING, LogMessage.of(function.get()));
        }
    }

    @Override
    public void warn(Object format, Object... arguments) {
        if (logger.isLoggable(Level.WARNING)) {
            log(Level.WARNING, LogMessage.of(format, arguments));
        }
    }

    @Override
    public void warn(Object message, Throwable t) {
        if (logger.isLoggable(Level.WARNING)) {
            logger.logrb(Level.WARNING, (String) null, (String) null, (ResourceBundle) null, String.valueOf(message),
                    t);
        }
    }

    @Override
    public Object unwrap() {
        return logger;
    }
}
