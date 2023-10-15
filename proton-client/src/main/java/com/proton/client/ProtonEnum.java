package com.proton.client;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class ProtonEnum {
    public static final ProtonEnum EMPTY = new ProtonEnum(Collections.emptyList());

    public static ProtonEnum of(Class<? extends Enum> clazz) {
        if (clazz == null || !Enum.class.isAssignableFrom(clazz)) {
            return EMPTY;
        }

        Enum<?>[] constants = clazz.getEnumConstants();
        int size = constants.length;
        String[] names = new String[size];
        int[] values = new int[size];
        int i = 0;
        for (Enum<?> e : clazz.getEnumConstants()) {
            names[i] = e.name();
            values[i] = e.ordinal();
            i++;
        }

        return new ProtonEnum(names, values);
    }

    private final int size;
    private final String[] names;
    private final int[] values;

    protected ProtonEnum(Collection<String> params) {
        size = params.size();
        names = new String[size];
        values = new int[size];

        int i = 0;
        for (String p : params) {
            int index = p.lastIndexOf('=');
            if (index > 0) {
                names[i] = ProtonUtils.unescape(p.substring(0, index));
                values[i] = Integer.parseInt(p.substring(index + 1));
            } else {
                throw new IllegalArgumentException("Invalid enum entry: " + p);
            }
            i++;
        }
    }

    protected ProtonEnum(String[] names, int[] values) {
        if (names == null || values == null) {
            throw new IllegalArgumentException("Non-null names and values are required");
        } else if (names.length != values.length) {
            throw new IllegalArgumentException("Names and values should have same length");
        }

        this.size = names.length;
        this.names = names;
        this.values = values;
    }

    public String validate(String name) {
        for (int i = 0; i < size; i++) {
            if (names[i].equals(name)) {
                return name;
            }
        }

        throw new IllegalArgumentException("Unknown enum name: " + name);
    }

    public int validate(int value) {
        for (int i = 0; i < size; i++) {
            if (values[i] == value) {
                return value;
            }
        }

        throw new IllegalArgumentException("Unknown enum value: " + value);
    }

    public String name(int value) {
        for (int i = 0; i < size; i++) {
            if (values[i] == value) {
                return names[i];
            }
        }

        throw new IllegalArgumentException("Unknown enum value: " + value);
    }

    public int value(String name) {
        for (int i = 0; i < size; i++) {
            if (names[i].equals(name)) {
                return values[i];
            }
        }

        throw new IllegalArgumentException("Unknown enum name: " + name);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + size;
        result = prime * result + Arrays.hashCode(names);
        result = prime * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ProtonEnum other = (ProtonEnum) obj;
        return size == other.size && Arrays.equals(names, other.names) && Arrays.equals(values, other.values);
    }

    public String toSqlException() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; i++) {
            builder.append('\'').append(ProtonUtils.escape(names[i], '\'')).append('\'').append('=')
                    .append(values[i]).append(',');
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }
}
