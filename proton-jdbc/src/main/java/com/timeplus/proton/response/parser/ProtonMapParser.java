package com.timeplus.proton.response.parser;

import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import com.timeplus.proton.domain.ProtonDataType;
import com.timeplus.proton.response.ByteFragment;
import com.timeplus.proton.response.ProtonColumnInfo;

@SuppressWarnings("rawtypes")
final class ProtonMapParser extends ProtonValueParser<Map> {

    private static ProtonMapParser instance;

    static ProtonMapParser getInstance() {
        if (instance == null) {
            instance = new ProtonMapParser();
        }
        return instance;
    }

    private ProtonMapParser() {
        // prevent instantiation
    }

    int readPart(ProtonDataType type, String str, int startPosition, int len, StringBuilder sb, char stopChar) {
        Deque<Character> stack = new ArrayDeque<>();
        stack.push('\0');
        char lastChar = '\0';
        for (int i = startPosition; i < len; i++) {
            char ch = str.charAt(startPosition = i);

            if (lastChar == '\0') {
                if (Character.isWhitespace(ch)) {
                    continue;
                }

                if (ch == stopChar) {
                    break;
                }

                switch (ch) {
                    case '\'':
                        if (lastChar != '\0') {
                            stack.push(lastChar);
                        }
                        lastChar = ch;
                        if (type != ProtonDataType.String) {
                            sb.append(ch);
                        }
                        break;
                    case '{':
                        if (lastChar != '\0') {
                            stack.push(lastChar);
                        }
                        lastChar = '}';
                        sb.append(ch);
                        break;
                    case '(':
                        if (lastChar != '\0') {
                            stack.push(lastChar);
                        }
                        lastChar = ')';
                        sb.append(ch);
                        break;
                    case '[':
                        if (lastChar != '\0') {
                            stack.push(lastChar);
                        }
                        lastChar = ']';
                        sb.append(ch);
                        break;
                    case '}':
                        return i + 1;
                    default:
                        sb.append(ch);
                        break;
                }
            } else if (lastChar == '\'') { // quoted
                if (ch != '\'' || type != ProtonDataType.String) {
                    sb.append(ch);
                }
                if (i + 1 < len) {
                    char nextChar = str.charAt(i + 1);
                    if (ch == '\\') {
                        sb.append(nextChar);
                        i++;
                    } else if (ch == '\'' && nextChar == ch) {
                        sb.append(ch).append(nextChar);
                        i++;
                    } else if (ch == '\'') {
                        lastChar = stack.pop();
                    }
                }
            } else if (lastChar == '}' || lastChar == ')' || lastChar == ']') {
                if (ch == lastChar) {
                    lastChar = stack.pop();
                }
                sb.append(ch);
            }
        }

        return startPosition;
    }

    @Override
    public Map parse(ByteFragment value, ProtonColumnInfo columnInfo, TimeZone resultTimeZone) throws SQLException {
        if (value.isNull()) {
            return null;
        }

        ProtonColumnInfo keyInfo = Objects.requireNonNull(columnInfo.getKeyInfo());
        ProtonColumnInfo valueInfo = Objects.requireNonNull(columnInfo.getValueInfo());

        ProtonValueParser<?> keyParser = ProtonValueParser
                .getParser(keyInfo.getProtonDataType().getJavaClass());
        ProtonValueParser<?> valueParser = ProtonValueParser
                .getParser(valueInfo.getProtonDataType().getJavaClass());

        String str = value.asString();
        int len = str == null ? 0 : str.length();
        if (len < 2) {
            return Collections.emptyMap();
        }

        Map<Object, Object> map = new LinkedHashMap<>();

        int part = -1; // -1 - uncertain, 0 - key, 1 - value
        StringBuilder sb = new StringBuilder();
        Object k = null;
        Object v = null;
        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);

            if (Character.isWhitespace(ch)) { // skip whitespaces
                continue;
            }

            if (part == -1) {
                if (ch == '{') {
                    part = 0;
                    continue;
                } else {
                    throw new IllegalArgumentException("Invalid map. Expect '{' but we got '" + ch + "' at " + i);
                }
            }

            if (ch == '}') {
                // TODO check if there's any pending characters
                break;
            }

            if (part == 0) { // reading key(String or Integer)
                i = readPart(keyInfo.getProtonDataType(), str, i, len, sb, ':');
                k = keyParser.parse(ByteFragment.fromString(sb.toString()), keyInfo, resultTimeZone);

                part = 1;
                sb.setLength(0);
            } else { // reading value(String, Integer or Array)
                i = readPart(valueInfo.getProtonDataType(), str, i, len, sb, ',');
                v = valueParser.parse(ByteFragment.fromString(sb.toString()), valueInfo, resultTimeZone);
                map.put(k, valueInfo.isArray() && v != null ? ((Array) v).getArray() : v);

                part = 0;
                sb.setLength(0);
            }
        }

        return Collections.unmodifiableMap(map);
    }

    @Override
    protected Map getDefaultValue() {
        return Collections.emptyMap();
    }
}
