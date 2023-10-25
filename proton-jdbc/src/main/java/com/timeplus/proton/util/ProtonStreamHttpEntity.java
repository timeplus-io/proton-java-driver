package com.timeplus.proton.util;

import org.apache.http.entity.AbstractHttpEntity;
import com.timeplus.proton.settings.ProtonProperties;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.TimeZone;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ProtonStreamHttpEntity extends AbstractHttpEntity {

    private final ProtonStreamCallback callback;
    private final TimeZone timeZone;
    private final ProtonProperties properties;

    public ProtonStreamHttpEntity(ProtonStreamCallback callback, TimeZone timeZone, ProtonProperties properties) {
        this.timeZone = timeZone;
        this.callback = Objects.requireNonNull(callback);
        this.properties = properties;
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return null;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        ProtonRowBinaryStream stream = new ProtonRowBinaryStream(out, timeZone, properties);
        callback.writeTo(stream);
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
