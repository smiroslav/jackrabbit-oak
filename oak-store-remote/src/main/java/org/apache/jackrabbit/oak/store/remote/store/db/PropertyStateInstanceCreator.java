package org.apache.jackrabbit.oak.store.remote.store.db;

import com.google.gson.InstanceCreator;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

import javax.jcr.PropertyType;
import java.lang.reflect.Type;

public class PropertyStateInstanceCreator implements InstanceCreator<PropertyState> {
    @Override
    public PropertyState createInstance(Type type) {
        return  PropertyStates.createProperty("t", "t", PropertyType.STRING);
    }
}
