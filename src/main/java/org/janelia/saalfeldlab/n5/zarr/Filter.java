/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
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
