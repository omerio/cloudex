/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2014, Ecarf.io
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

package io.cloudex.framework.utils;

import io.cloudex.framework.exceptions.ClassInstantiationException;
import io.cloudex.framework.exceptions.InstancePopulationException;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.WrapDynaBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public final class ObjectUtils {

    private static final Log log = LogFactory.getLog(ObjectUtils.class);
    
    public static final Gson GSON = new Gson();

    /**
     * Create an instance of the provided class
     * @param classOfT - the type of the object to return
     * @param className - the full className
     * @return an instantiated object casted to type T
     * @throws ClassInstantiationException if the creation of the instance fails
     */
    @SuppressWarnings("unchecked")
    public static <T> T createInstance(Class<T> classOfT, String className) throws ClassInstantiationException {

        T object = null;
        try {
            Class<?> clazz = Class.forName(className);
            object = (T) clazz.newInstance();

        } catch(Exception e) {
            log.error("Failed to instantiate object from className: " + className, e);
            throw new ClassInstantiationException("Invalid className: " + className, e);
        }
        return object;
    }
    
    
    /**
     * Wrapper for BeanUtils.populate, validates that bean has all the properties referenced in
     * the properties map
     * @param bean - the bean to populate
     * @param properties - a map of key value pairs used to populate bean
     * @throws InstancePopulationException if the population of the bean fails
     */
    public static void populate(Object bean, Map<String, ? extends Object> properties) 
            throws InstancePopulationException {
        populate(bean, properties, true);
    }

    /**
     * Wrapper for BeanUtils.populate, validates that bean has all the properties referenced in
     * the properties map
     * @param bean - the bean to populate
     * @param properties - a map of key value pairs used to populate bean
     * @param validate - validate that all provided properties are in bean
     * @throws InstancePopulationException if the population of the bean fails
     */
    public static void populate(Object bean, Map<String, ? extends Object> properties, boolean validate)  
            throws InstancePopulationException {

        try {
            
            if(validate) {
                DynaBean dynaBean = new WrapDynaBean(bean);

                // ensure all the properties in the map are present
                for(String property: properties.keySet()) {
                    dynaBean.get(property);
                    // if no present it will throw IllegalArgumentException
                }
            }
            
            BeanUtils.populate(bean, properties);
            
        } catch (Exception e) {
            log.error("Failed to populate instance: " + bean + ", from map: " + properties, e);
            throw new InstancePopulationException("Unable to populate instance: " + bean, e);
        }
    }
    
    /**
     * Convert a json string to map
     * @param json
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> jsonToMap(String json) {
        return GSON.fromJson(json, HashMap.class);
    }
    
    /**
     * comma separated string to set
     * @param csv
     * @return
     */
    public static Set<String> csvToSet(String csv) {
        Set<String> tokens = new HashSet<>();
        if(StringUtils.isNotBlank(csv)) {   
            String [] tokensArr = StringUtils.split(csv, ',');
            // clean up the tokens
            //tokens = Sets.newHashSet(tokensArr);
            for(String token: tokensArr) {
                tokens.add(token.trim());
            }
        } 
        return tokens; 
    }
    
    /**
     * retrive the value of a json property
     * @param json
     * @param key
     * @return
     */
    public static String getStringPropertyFromJson(String json, String key) {
        Map<String, Object> map = jsonToMap(json);
        
        return (String) map.get(key);
    }
    
}
