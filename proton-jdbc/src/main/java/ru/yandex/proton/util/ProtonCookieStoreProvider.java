package ru.yandex.proton.util;

import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import ru.yandex.proton.settings.ProtonProperties;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProtonCookieStoreProvider {
    private static final Map<String, CookieStore> cookieStoreMap = new ConcurrentHashMap<>();

    public CookieStore getCookieStore(ProtonProperties properties) {
        return hasValidProperties(properties) && properties.isUseSharedCookieStore() ?
                cookieStoreMap.computeIfAbsent(getCookieStoreKey(properties), k -> new BasicCookieStore()) :
                null;
    }

    private boolean hasValidProperties(ProtonProperties properties) {
        return properties != null
                && !Utils.isNullOrEmptyString(properties.getHost())
                && properties.getPort() > 0
                && !Utils.isNullOrEmptyString(properties.getDatabase());
    }

    private String getCookieStoreKey(ProtonProperties properties) {
        return String.format("%s:%s/%s", properties.getHost(), properties.getPort(), properties.getDatabase());
    }
}
