package org.apache.jackrabbit.oak.store.remote.store.db;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

import java.lang.reflect.Type;

public class PropertyStateSerializer implements JsonSerializer<PropertyState> {
    @Override
    public JsonElement serialize(PropertyState src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", src.getName());
        jsonObject.addProperty("isArray", src.isArray());
        jsonObject.addProperty("type", src.getType().tag());
        if (src.isArray()) {
            JsonArray jsonArray = new JsonArray();
            for (int i = 0; i < src.count(); i++) {
                //jsonArray.add(src.getValue(org.apache.jackrabbit.oak.api.Type.STRING, i));
                jsonArray.add(src.getValue(org.apache.jackrabbit.oak.api.Type.fromTag(src.getType().tag(), false), i).toString());
            }
            jsonObject.add("value", jsonArray);
        } else {
            jsonObject.addProperty("value", src.getValue(org.apache.jackrabbit.oak.api.Type.STRING));
        }
        return jsonObject;
    }
}
