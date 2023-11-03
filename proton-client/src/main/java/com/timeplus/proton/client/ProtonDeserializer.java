package com.timeplus.proton.client;

import java.io.IOException;

import com.timeplus.proton.client.data.ProtonEmptyValue;

/**
 * Functional interface for deserialization.
 */
@FunctionalInterface
public interface ProtonDeserializer<T extends ProtonValue> {
    /**
     * Default deserializer simply returns empty value.
     */
    ProtonDeserializer<ProtonValue> EMPTY_VALUE = (v, f, c, i) -> ProtonEmptyValue.INSTANCE;

    /**
     * Default deserializer throws IOException to inform caller deserialization is
     * not supported.
     */
    ProtonDeserializer<ProtonValue> NOT_SUPPORTED = (v, f, c, i) -> {
        throw new IOException(c.getOriginalTypeName() + " is not supported");
    };

    /**
     * Deserializes data read from input stream.
     *
     * @param ref    wrapper object can be reused, could be null(always return new
     *               wrapper object)
     * @param config non-null configuration
     * @param column non-null type information
     * @param input  non-null input stream
     * @return deserialized value which might be the same instance as {@code ref}
     * @throws IOException when failed to read data from input stream
     */
    T deserialize(T ref, ProtonConfig config, ProtonColumn column, ProtonInputStream input)
            throws IOException;
}
