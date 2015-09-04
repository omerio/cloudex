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

package io.cloudex.framework.config;

import io.cloudex.framework.types.ExecutionMode;
import io.cloudex.framework.utils.ObjectUtils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.google.gson.stream.JsonReader;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Job implements Serializable {

    private static final long serialVersionUID = 7460340670508024439L;

    private String id;
    
    private String description;

    @NotNull
    @Size(min = 1)
    private Map<String, Object> data;
    
    @NotNull
    private VmConfig vmConfig;

    private ExecutionMode mode;

    @NotNull
    @Size(min = 1)
    private List<TaskConfig> tasks;


    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the data
     */
    public Map<String, Object> getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    /**
     * @return the mode
     */
    public ExecutionMode getMode() {
        return mode;
    }

    /**
     * @param mode the mode to set
     */
    public void setMode(ExecutionMode mode) {
        this.mode = mode;
    }

    /**
     * @return the tasks
     */
    public List<TaskConfig> getTasks() {
        return tasks;
    }

    /**
     * @return the vmConfig
     */
    public VmConfig getVmConfig() {
        return vmConfig;
    }

    /**
     * @param vmConfig the vmConfig to set
     */
    public void setVmConfig(VmConfig vmConfig) {
        this.vmConfig = vmConfig;
    }

    /**
     * @param tasks the tasks to set
     */
    public void setTasks(List<TaskConfig> tasks) {
        this.tasks = tasks;
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Convert to JSON
     * @return json representation of this object.
     */
    public String toJson() {
        return ObjectUtils.GSON.toJson(this);
    }

    /**
     * De-serialize a job instance from a json file.
     * @param jsonFile - a file path
     * @return a Job instance
     * @throws FileNotFoundException if the file is not found
     * @throws IOException if the json conversion fails
     */
    public static Job fromJsonFile(String jsonFile) throws FileNotFoundException, IOException {
        try(FileReader reader = new FileReader(jsonFile)) {
            return ObjectUtils.GSON.fromJson(new JsonReader(reader), Job.class);
        }
    }

    /**
     * De-serialize a job instance from a json string.
     * @param json - a json string
     * @return a Job instance
     */
    public static Job fromJsonString(String json) {
        return ObjectUtils.GSON.fromJson(json, Job.class);
    }
    
    /**
     * check if this instance is valid
     * @return true if valid
     */
    public boolean valid() {
        boolean valid = true;
        valid = ObjectUtils.isValid(Job.class, this) && this.vmConfig.valid();
        
        for(TaskConfig taskConfig: this.tasks) {
            valid = valid && taskConfig.valid();
            if(!valid) {
                break;
            }
        }
        return valid;
    }
    
    /**
     * Get any validation errors
     * @return a list of validation messages
     */
    public List<String> getValidationErrors() {
        List<String> messages = ObjectUtils.getValidationErrors(Job.class, this);
        
        if(messages.isEmpty()) {
            messages.addAll(this.vmConfig.getValidationErrors());
            
            for(TaskConfig taskConfig: this.tasks) {
                messages.addAll(taskConfig.getValidationErrors());
            }
            
        }
        return messages;
    }

}
