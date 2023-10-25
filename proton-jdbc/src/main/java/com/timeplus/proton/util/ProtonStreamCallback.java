package com.timeplus.proton.util;

import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public interface ProtonStreamCallback {
    void writeTo(ProtonRowBinaryStream stream) throws IOException;
}
