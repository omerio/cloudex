/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2015, cloudex.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.cloudex.framework.components;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Task execution context, responsible for resolving values.
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Context {

    public static final String VARIABLE_PREFIX = "#";

    // Tasks execution context
    private Map<String, Object> context;
    
    private Map<String, Object> readOnly;

    /**
     * default constructor
     */
    public Context() {
        this.context = new HashMap<>();
        this.readOnly = new HashMap<>();
    }

    /**
     * Initialize a context from some data
     * @param data
     */
    public Context(Map<String, Object> data) {
        this();

        this.context.putAll(data);
    }

    /**
     * @param key
     * @param value
     * @return
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    public Object putReadOnly(String key, Object value) {
        return readOnly.put(key, value);
    }
    
    /**
     * Resolve the underlying key name, e.g. #key returns key
     * @param key - the key reference
     * @return underlying key
     */
    public static String resolveKey(String key) {
        int size = key.length();
        if(key.startsWith(VARIABLE_PREFIX) && (size > 1)) {
            key = StringUtils.right(key, size - 1);
        }
        return key;
    }
    
    /**
     * Get a reference to the provided key
     * @param key - they key
     * @return key reference e.g. #key
     */
    public static String getKeyReference(String key) {
        return VARIABLE_PREFIX + key;
    }

    /**
     * Resolve a value using a valueKey, valueKey starts with VARIABLE_PREFIX in the form #valueKey
     * @param valueKey - a key of a value 
     * @return - the resolved value
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Object resolveValue(final String valueKey) {

        Validate.notNull(valueKey, "valueKey is required");

        Object value = null;
        String key = resolveKey(valueKey);

        if(valueKey.equals(key)) {
            // nothing to resolve, just return it as it's
            value = valueKey;        

        } else {
            
            // resolve from the Readonly first
            if(this.readOnly.keySet().contains(key)) {
                value = this.readOnly.get(key);
                
                if(value instanceof Collection) {
                    value = Collections.unmodifiableCollection((Collection) value);
                }
                
            } else {
                value = this.context.get(key);
            }
        }

        return value;
    }

    /**
     * Resolve a map of values using the key as the key on the returned values map and using the valueKey as
     * the key to use to resolve the value from the context
     * @param valuesKeys - a map of key, valueKey
     * @return - a map of values
     */
    public Map<String, Object> resolveValues(final Map<String, String> valuesKeys) {

        Validate.notNull(valuesKeys, "valuesKeys is required");
        Map<String, Object> values = new HashMap<>();

        for(Entry<String, String> entry: valuesKeys.entrySet()) {
            values.put(entry.getKey(), this.resolveValue(entry.getValue()));
        }

        return values;
    }

    /**
     * @return
     * @see java.util.Map#size()
     */
    public int size() {
        return context.size();
    }

    /**
     * @return
     * @see java.util.Map#isEmpty()
     */
    public boolean isEmpty() {
        return context.isEmpty();
    }

    /**
     * @param key
     * @return
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key) {
        return context.containsKey(key);
    }

    /**
     * @param value
     * @return
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    public boolean containsValue(Object value) {
        return context.containsValue(value);
    }

    /**
     * @param key
     * @return
     * @see java.util.Map#get(java.lang.Object)
     */
    public Object get(Object key) {
        return context.get(key);
    }

    /**
     * @param key
     * @param value
     * @return
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    public Object put(String key, Object value) {
        return context.put(key, value);
    }

    /**
     * @param key
     * @return
     * @see java.util.Map#remove(java.lang.Object)
     */
    public Object remove(Object key) {
        return context.remove(key);
    }

    /**
     * @param m
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void putAll(Map<? extends String, ? extends Object> m) {
        context.putAll(m);
    }

    /**
     * 
     * @see java.util.Map#clear()
     */
    public void clear() {
        context.clear();
    }

    /**
     * @return
     * @see java.util.Map#keySet()
     */
    public Set<String> keySet() {
        return context.keySet();
    }

    /**
     * @return
     * @see java.util.Map#values()
     */
    public Collection<Object> values() {
        return context.values();
    }

    /**
     * @return
     * @see java.util.Map#entrySet()
     */
    public Set<Entry<String, Object>> entrySet() {
        return context.entrySet();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("context", context)
            .append("readOnly", readOnly)
            .toString();
    }


}
