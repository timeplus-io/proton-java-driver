package ru.yandex.proton;

import ru.yandex.proton.settings.ProtonQueryParam;

import java.util.*;

@SuppressWarnings("unchecked")
class ConfigurableApi<T> {

    protected final ProtonStatementImpl statement;
    private Map<ProtonQueryParam, String> additionalDBParams = new HashMap<ProtonQueryParam, String>();
    private Map<String, String> additionalRequestParams = new HashMap<String, String>();

    ConfigurableApi(ProtonStatementImpl statement) {
        this.statement = statement;
    }

    Map<String, String> getRequestParams() {
        return additionalRequestParams;
    }

    Map<ProtonQueryParam, String> getAdditionalDBParams() {
        return additionalDBParams;
    }

    public T addDbParam(ProtonQueryParam param, String value) {
        additionalDBParams.put(param, value);
        return (T) this;
    }

    public T removeDbParam(ProtonQueryParam param) {
        additionalDBParams.remove(param);
        return (T) this;
    }

    public T withDbParams(Map<ProtonQueryParam, String> dbParams) {
        this.additionalDBParams = new HashMap<ProtonQueryParam, String>();
        if (null != dbParams) {
            additionalDBParams.putAll(dbParams);
        }
        return (T) this;
    }

    public T options(Map<String, String> params) {
        additionalRequestParams = new HashMap<String, String>();
        if (null != params) {
            additionalRequestParams.putAll(params);
        }
        return (T) this;
    }

    public T option(String key, String value) {
        additionalRequestParams.put(key, value);
        return (T) this;
    }

    public T removeOption(String key) {
        additionalRequestParams.remove(key);
        return (T) this;
    }

}
