/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.admin.json;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * JSON serializer for controller metadata.
 */
public class ControllerMetadataJsonSerializer {

    /**
     * Method to convert JSON string into object of the given type.
     *
     * @param jsonValue The JSON string.
     * @param type      The object's class.
     * @param <T>       The type of the object.
     * @return The object embedded in the given JSON string.
     */
    public <T> T fromJson(String jsonValue, Class<T> type) {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(Map.Entry.class, new MapEntrySerializer())
                .registerTypeAdapter(ImmutableList.class, new ImmutableListDeserializer())
                .registerTypeAdapter(ImmutableSet.class, new ImmutableSetDeserializer())
                .registerTypeAdapter(ImmutableMap.class, new ImmutableMapDeserializer())
                .create();
        return gson.fromJson(jsonValue, type);
    }

    /**
     * Method to convert the given object into a JSON string.
     *
     * @param src The Object.
     * @return A JSON string containing the given object.
     */
    public String toJson(Object src) {
        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .serializeNulls()
                .enableComplexMapKeySerialization()
                .registerTypeAdapter(Map.Entry.class, new MapEntrySerializer())
                .create();
        return gson.toJson(src);
    }

    /**
     * A custom JSON deserializer for the {@link ImmutableList} type.
     */
    private static class ImmutableListDeserializer implements JsonDeserializer<ImmutableList<?>> {

        @Override
        public ImmutableList<?> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            Type[] typeArguments = ((ParameterizedType) typeOfT).getActualTypeArguments();
            Type parametrizedType = collectionOf(typeArguments[0]).getType();
            Collection<?> collection = context.deserialize(json, parametrizedType);
            return ImmutableList.copyOf(collection);
        }
    }

    /**
     * A custom JSON deserializer for the {@link ImmutableSet} type.
     */
    private static class ImmutableSetDeserializer implements JsonDeserializer<ImmutableSet<?>> {

        @Override
        public ImmutableSet<?> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            Type[] typeArguments = ((ParameterizedType) typeOfT).getActualTypeArguments();
            Type parametrizedType = collectionOf(typeArguments[0]).getType();
            Collection<?> collection = context.deserialize(json, parametrizedType);
            return ImmutableSet.copyOf(collection);
        }
    }

    /**
     * A custom JSON deserializer for the {@link ImmutableMap} type.
     */
    private static class ImmutableMapDeserializer implements JsonDeserializer<ImmutableMap<?, ?>> {

        @Override
        public ImmutableMap<?, ?> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            Type[] typeArguments = ((ParameterizedType) typeOfT).getActualTypeArguments();
            Type parametrizedType = hashMapOf(typeArguments[0], typeArguments[1]).getType();
            Map<?, ?> map = context.deserialize(json, parametrizedType);
            return ImmutableMap.copyOf(map);
        }
    }

    /**
     * A custom JSON serializer and deserializer for the {@link Map.Entry} of two {@link Double}s.
     */
    private static class MapEntrySerializer implements JsonSerializer<Map.Entry<Double, Double>>, JsonDeserializer<Map.Entry<Double, Double>> {

        @Override
        public Map.Entry<Double, Double> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            Map<String, Double> map = context.deserialize(json, Map.class);
            System.out.println("here: " + map);
            String k = map.keySet().stream().iterator().next();
            return Map.entry(Double.parseDouble(k), map.get(k));
        }

        @Override
        public JsonElement serialize(Map.Entry<Double, Double> src, Type typeOfSrc, JsonSerializationContext context) {
            Map<Double, Double> map = new HashMap<>();
            map.put(src.getKey(), src.getValue());
            return context.serialize(map);
        }
    }

    private static <E> TypeToken<Collection<E>> collectionOf(Type type) {
        // Generate a Collection<E> TypeToken given the Type of the element.
        TypeParameter<E> elementTypeParameter = new TypeParameter<E>() { };
        return new TypeToken<Collection<E>>() { }
        .where(elementTypeParameter, typeTokenOf(type));
    }

    private static <K, V> TypeToken<HashMap<K, V>> hashMapOf(Type key, Type value) {
        // Generate a HashMap<K, V> TypeToken given the Types of the key and value.
        TypeParameter<K> keyTypeParameter = new TypeParameter<K>() { };
        TypeParameter<V> valueTypeParameter = new TypeParameter<V>() { };
        return new TypeToken<HashMap<K, V>>() { }
        .where(keyTypeParameter, typeTokenOf(key))
        .where(valueTypeParameter, typeTokenOf(value));
    }

    @SuppressWarnings("unchecked")
    private static <E> TypeToken<E> typeTokenOf(Type type) {
        return (TypeToken<E>) TypeToken.of(type);
    }
}
