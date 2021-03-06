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

package io.cloudex.framework.cloud.entities;

import io.cloudex.framework.types.CodeLocation;
import io.cloudex.framework.types.ProcessorStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A collection of cloud instance metadata used by cloudex. 
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class VmMetaData {

    private static final Log log = LogFactory.getLog(VmMetaData.class);

    // maximum size of metadata
    //public static final int MAX_METADATA_SIZE = 32768;
    
    // a meta data entry created as a replacement of a long meta data entry
    public static final String LONG_METADATA_FILE_Suffix = "File";
    
    // cloud-ex specific metadata
    public static final String CLOUDEX_PREFIX = "cloudex-";
    
    public static final String USER_PREFIX = "user-";

    // the prefix of the vm name
    public static final String CLOUDEX_VM_PREFIX = CLOUDEX_PREFIX + "processor-";

    // the task for the processor
    public static final String CLOUDEX_TASK_CLASS = CLOUDEX_PREFIX + "task-class";

    public static final String CLOUDEX_TASK_CODE_TYPE = CLOUDEX_PREFIX + "task-code-type";

    public static final String CLOUDEX_TASK_CODE_URL = CLOUDEX_PREFIX + "task-code-url";

    // the status of the processor, ready, busy or error
    public static final String CLOUDEX_STATUS = CLOUDEX_PREFIX + "status";

    // cloudex-exception
    public static final String CLOUDEX_EXCEPTION = CLOUDEX_PREFIX + "exception";

    // cloudex-message
    public static final String CLOUDEX_MESSAGE = CLOUDEX_PREFIX + "message";

    private Map<String, Object> attributes;

    private String fingerprint;

    /**
     * 
     */
    public VmMetaData() {
        super();
        this.attributes = new HashMap<>();
    }

    /**
     * @param attributes
     */
    public VmMetaData(Map<String, Object> attributes) {
        super();
        this.attributes = attributes;
    }

    /**
     * @param attributes
     */
    public VmMetaData(Map<String, Object> attributes, String fingerprint) {
        super();
        this.attributes = attributes;
        this.fingerprint = fingerprint;
    }
    
    /**
     * Get the status
     * @return
     */
    public String getStatus() {
        return (String) this.attributes.get(CLOUDEX_STATUS);
    }
    
    /**
     * Get the processor status enum value
     * @return
     */
    public ProcessorStatus getProcessorStatus() { 
        ProcessorStatus processStatus = null;
        String status = this.getStatus();
        if(status != null) {
            try {
                processStatus = ProcessorStatus.valueOf(status);
                
            } catch(IllegalArgumentException e) {
                log.error("Failed to parse processor status", e);
            }
        }
        
        return processStatus;
    }
    
    /**
     * Set the processor status in this metadata
     * @param status - the process status {@link ProcessorStatus}
     */
    public void setProcessorStatus(ProcessorStatus status) {
        this.attributes.put(CLOUDEX_STATUS, status.toString());
    }

    /**
     * Get the task code type 
     * @return
     */
    public CodeLocation getTaskCodeType() {
        CodeLocation codeType = null;
        if(this.attributes.get(CLOUDEX_TASK_CODE_TYPE) != null) {
            try {
                codeType = CodeLocation.valueOf((String) this.attributes.get(CLOUDEX_TASK_CODE_TYPE));

            } catch(IllegalArgumentException e) {
                log.error("Failed to parse task type", e);
            }
        }
        return codeType;
    }
    
    /**
     * Set the processor code location type
     * @param codeType - the code location {@link CodeLocation}
     */
    public void setTaskCodeType(CodeLocation codeType) {
        this.attributes.put(CLOUDEX_TASK_CODE_TYPE, codeType.toString());
    }

    /**
     * Get the processor status
     * @return
     */
    public String getTaskClass() {
        return (String) this.attributes.get(CLOUDEX_TASK_CLASS);
    }
    
    /**
     * Set the processor task class
     * @param taskClass - the class name to set
     */
    public void setTaskClass(String taskClass) {
        this.attributes.put(CLOUDEX_TASK_CLASS, taskClass);
    }

    /**
     * Get the url of the the task code, only if status is set to remote
     * @return
     */
    public String getTaskCodeUrl() {
        return (String) this.attributes.get(CLOUDEX_TASK_CODE_URL);
    }

    /**
     * Get the exception if any
     * @return
     */
    public String getException() {
        return (String) this.attributes.get(CLOUDEX_EXCEPTION);
    }
    
    /**
     * Sets the exception class name
     * @param exp - the exception
     */
    public void setException(Exception exp) {
        this.attributes.put(CLOUDEX_EXCEPTION, exp.getClass().getName());
    }

    /**
     * Get the message if any
     * @return
     */
    public String getMessage() {
        return (String) this.attributes.get(CLOUDEX_MESSAGE);
    }
    
    /**
     * Sets the processor error message 
     * @param message - the error message
     */
    public void setMessage(String message) {
        this.attributes.put(CLOUDEX_MESSAGE, message);
    }
    
    /**
     * From an exception populate a cloudex metadata error
     * @param exception - the exception
     */
    public void exceptionToCloudExError(Exception exception) {
        this.setProcessorStatus(ProcessorStatus.ERROR);
        this.setException(exception);
        this.setMessage(exception.getMessage());
    }
    
    /**
     * Return a map of all the user meta data
     * @return the user provided attributes
     */
    public Map<String, String> getUserMetaData() {
        Map<String, String> userData = new HashMap<>();
        
        String property;
        
        for(String key: this.attributes.keySet()) {
            
            if(key.startsWith(USER_PREFIX)) {
                // we strip a user- prefix on the metadata names then
                // just use the result as the property name
                property = StringUtils.removeStart(key, USER_PREFIX);
                
                userData.put(property, this.getValue(key));
            }
        }
        
        return userData;
    }
    

    /**
     * 
     * @param key
     * @return
     */
    public String getValue(String key) {
        return (String) this.attributes.get(key);
    }

    /**
     * add a value to the metadata, return this instance for chaining
     * @param key
     * @param value
     */
    public VmMetaData addValue(String key, String value) {
        this.attributes.put(key, value);
        return this;
    }
        
    /**
     * add a user value to the metadata, return this instance for chaining
     * @param key
     * @param value
     */
    public VmMetaData addUserValue(String key, String value) {
        this.attributes.put(USER_PREFIX + key, value);
        return this;
    }
    
    /**
     * Add a map of user values to the metadata, return this instance for chaining
     * @param values - the values map
     * @return this instance {@link VmMetaData}
     */
    public VmMetaData addUserValues(Map<String, String> values) {
        for(Entry<String, String> entry: values.entrySet()) {
            this.addUserValue(entry.getKey(), entry.getValue());
        }
        return this;
    }

    /**
     * 
     * @see java.util.Map#clear()
     */
    public void clearValues() {
        attributes.clear();
    }

    /**
     * @return the attributes
     */
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    /**
     * @return the fingerprint
     */
    public String getFingerprint() {
        return fingerprint;
    }

    /**
     * @param fingerprint the fingerprint to set
     */
    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }
    
    /**
     * Some cloud providers require that a follow up metadata to contain a fingerprint signature
     * @param followUp - the metadata to update
     * @return metadata with fingerprint populated
     */
    public VmMetaData getFollowUp(VmMetaData followUp) {
        if(followUp == null) {
            followUp = new VmMetaData();
        }
        followUp.setFingerprint(this.getFingerprint());
        
        return followUp;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("fingerprint", fingerprint)
            .append("attributes", attributes)
            .toString();
    }

}
