package com.timeplus.proton.client;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Functional interface for serializtion.
 */
@FunctionalInterface
public interface ProtonSerializer<T extends ProtonValue> {
    /**
     * Default serializer simply does nothing.
     */
    ProtonSerializer<ProtonValue> DO_NOTHING = (v, f, c, o) -> {
    };

    /**
     * Default deserializer throws IOException to inform caller serialization is
     * not supported.
     */
    ProtonSerializer<ProtonValue> NOT_SUPPORTED = (v, f, c, o) -> {
        throw new IOException(c.getOriginalTypeName() + " is not supported");
    };

    /**
     * Writes serialized value to output stream.
     *
     * @param value  non-null value to be serialized
     * @param config non-null configuration
     * @param column non-null type information
     * @param output non-null output stream
     * @throws IOException when failed to write data to output stream
     */
    void serialize(T value, ProtonConfig config, ProtonColumn column, OutputStream output) throws IOException;
}
