package org.janelia.saalfeldlab.n5.zarr;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Filter types
 *
 * TODO implement some
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Michael Innerberger
 */
public interface Filter {

    String getId();

    // Note: the JSON (de-)serializer below is very much tailored to this filter, which serializes to "{"id":"vlen-utf8"}"
    // If additional filters are implemented, consider also changing the type adapter below
    Filter VLEN_UTF8 = new VLenStringFilter();

    class VLenStringFilter implements Filter {
        private static final String id = "vlen-utf8";
        @Override
        public String getId() {
            return id;
        }
    };

    static Filter fromString(final String id) {
        if (VLEN_UTF8.getId().equals(id))
                return VLEN_UTF8;
        return null;
    }

    JsonAdapter jsonAdapter = new JsonAdapter();

    class JsonAdapter implements JsonDeserializer<Filter>, JsonSerializer<Filter> {

        @Override
        public Filter deserialize(
                final JsonElement json,
                final Type typeOfT,
                final JsonDeserializationContext context) throws JsonParseException {

            final JsonElement jsonId = json.getAsJsonObject().get("id");
            if (jsonId == null)
                return null;

            final String stringId = jsonId.getAsString();
            return Filter.fromString(stringId);
        }

        @Override
        public JsonElement serialize(
                final Filter filter,
                final Type typeOfSrc,
                final JsonSerializationContext context) {

            final JsonObject serialization = new JsonObject();
            serialization.add("id", new JsonPrimitive(filter.getId()));
            return serialization;
        }
    }
}
