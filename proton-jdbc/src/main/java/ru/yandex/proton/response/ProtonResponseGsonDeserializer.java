package ru.yandex.proton.response;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class ProtonResponseGsonDeserializer implements JsonDeserializer<ProtonResponse> {

    @Override
    public ProtonResponse deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        ProtonResponse result = new ProtonResponse();
        JsonObject jsonObject = json.getAsJsonObject();

        JsonArray metaNode = jsonObject.getAsJsonArray("meta");
        if (metaNode != null) {
            List<ProtonResponse.Meta> meta = new ArrayList<>();
            metaNode.forEach(e -> meta.add(parseMeta(e)));
            result.setMeta(meta);
        }

        JsonArray dataNode = jsonObject.getAsJsonArray("data");
        if (dataNode != null) {
            List<List<String>> data = new ArrayList<>();
            dataNode.forEach(row -> {
                List<String> rowList = getAsStringArray(row);
                data.add(rowList);
            });
            result.setData(data);
        }

        JsonArray totalsNode = jsonObject.getAsJsonArray("totals");
        if (totalsNode != null) {
            List<String> totals = getAsStringArray(totalsNode);
            result.setTotals(totals);
        }

        JsonObject extremesNode = jsonObject.getAsJsonObject("extremes");
        if (extremesNode != null) {
            ProtonResponse.Extremes extremes = new ProtonResponse.Extremes();
            extremes.setMax(getAsStringArray(extremesNode.get("max")));
            extremes.setMin(getAsStringArray(extremesNode.get("min")));
            result.setExtremes(extremes);
        }

        JsonElement rowsNode = jsonObject.get("rows");
        if (rowsNode != null) {
            result.setRows(rowsNode.getAsInt());
        }

        JsonElement rows_before_limit_at_leastNode = jsonObject.get("rows_before_limit_at_least");
        if (rows_before_limit_at_leastNode != null) {
            result.setRows_before_limit_at_least(rows_before_limit_at_leastNode.getAsInt());
        }

        return result;
    }

    private List<String> getAsStringArray(JsonElement row) {
        JsonArray rowArray = row.getAsJsonArray();
        List<String> rowList = new ArrayList<>();
        rowArray.forEach(value -> {
            String valueStr = getAsString(value);
            rowList.add(valueStr);
        });
        return rowList;
    }

    private String getAsString(JsonElement value) {
        String valueStr;
        if (value.isJsonPrimitive()) {
            valueStr = value.getAsString();
        } else if (value.isJsonArray()) {
            valueStr = arrayToString(value);
        } else {
            throw new IllegalArgumentException("unexpected jsonElementType: " + value.toString());
        }
        return valueStr;
    }

    private ProtonResponse.Meta parseMeta(JsonElement e) {
        JsonObject metaObject = e.getAsJsonObject();
        ProtonResponse.Meta meta = new ProtonResponse.Meta();
        meta.setName(metaObject.get("name").getAsString());
        meta.setType(metaObject.get("type").getAsString());
        return meta;
    }

    private String arrayToString(JsonElement value) {
        return value.getAsJsonArray().toString();
    }
}
