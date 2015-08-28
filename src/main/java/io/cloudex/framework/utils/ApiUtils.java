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


package io.cloudex.framework.utils;

import io.cloudex.framework.cloud.VmMetaData;
import io.cloudex.framework.exceptions.ProcessorException;
import io.cloudex.framework.types.ProcessorStatus;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public final class ApiUtils {

    private static final Log log = LogFactory.getLog(ApiUtils.class);
    
    private static final String PROCESSOR_EXCEPTION = "EVM Exception: ";

    /**
     * Block for the number of provided seconds
     * @param seconds - the number of seconds to block
     */
    public static void block(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
            
        } catch (InterruptedException e1) {
            log.warn("wait interrupted", e1);
        }
    }
    
    /**
     * From an exception populate a cloudex metadata error
     * @param metadata
     * @param exception
     */
    public static void exceptionToCloudExError(VmMetaData metadata, Exception exception) {
        metadata.addValue(VmMetaData.CLOUDEX_STATUS, ProcessorStatus.ERROR.toString());
        metadata.addValue(VmMetaData.CLOUDEX_EXCEPTION, exception.getClass().getName());
        metadata.addValue(VmMetaData.CLOUDEX_MESSAGE, exception.getMessage());
    }
    
    /**
     * Return an IOException from the metaData error
     * @param metaData
     * @return
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static IOException exceptionFromCloudExError(VmMetaData metaData, String instanceId) {
        Class clazz = IOException.class;
        String message = metaData.getMessage();
        if(StringUtils.isNoneBlank(metaData.getException())) {
            try {
                clazz = Class.forName(metaData.getException());
            } catch (ClassNotFoundException e) {
                log.warn("failed to load exception class from evm");
            }
        }
        Exception cause;
        try {
            Constructor ctor = clazz.getDeclaredConstructor(String.class);
            ctor.setAccessible(true);
            cause = (Exception) ctor.newInstance(message);
        } catch (Exception e) {
            log.warn("failed to load exception class from evm");
            cause = new IOException(message);
        }
        return new ProcessorException(PROCESSOR_EXCEPTION + instanceId, cause, instanceId);
    }
}
