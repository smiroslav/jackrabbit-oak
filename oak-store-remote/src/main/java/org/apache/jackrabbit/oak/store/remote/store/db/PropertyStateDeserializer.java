package org.apache.jackrabbit.oak.store.remote.store.db;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PropertyStateDeserializer implements JsonDeserializer<PropertyState> {

    @Override
    public PropertyState deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObject = json.getAsJsonObject();

        String name = jsonObject.get("name").getAsString();
        boolean isArray = jsonObject.get("isArray").getAsBoolean();
        int type = jsonObject.get("type").getAsInt();

        if (isArray) {

            List<String> values =
                    StreamSupport.stream(jsonObject.get("value")
                            .getAsJsonArray().spliterator(), false)
                            .map(jsonElement -> jsonElement.getAsString()).collect(Collectors.toList());

            return PropertyStates.createProperty(name, values, org.apache.jackrabbit.oak.api.Type .fromTag(type, true));
        } else {
            return PropertyStates.createProperty(name, jsonObject.get("value").getAsString(), type);
        }

        //Type.fromTag(type, false)
    }
}
