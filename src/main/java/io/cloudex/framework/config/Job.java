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

import io.cloudex.framework.cloud.VmConfig;
import io.cloudex.framework.types.ExecutionMode;
import io.cloudex.framework.utils.ObjectUtils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.google.gson.stream.JsonReader;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Job implements Serializable {

    private static final long serialVersionUID = 7460340670508024439L;

    private String id;

    private Map<String, Object> data;
    
    private VmConfig vmConfig;

    private ExecutionMode mode;

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
    public final VmConfig getVmConfig() {
        return vmConfig;
    }

    /**
     * @param vmConfig the vmConfig to set
     */
    public final void setVmConfig(VmConfig vmConfig) {
        this.vmConfig = vmConfig;
    }

    /**
     * @param tasks the tasks to set
     */
    public void setTasks(List<TaskConfig> tasks) {
        this.tasks = tasks;
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
     * @param jsonFile
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static Job fromJsonFile(String jsonFile) throws FileNotFoundException, IOException {
        try(FileReader reader = new FileReader(jsonFile)) {
            return ObjectUtils.GSON.fromJson(new JsonReader(reader), Job.class);
        }
    }

    /**
     * De-serialize a job instance from a json string.
     * @param json
     * @return
     */
    public static Job fromJsonString(String json) {
        return ObjectUtils.GSON.fromJson(json, Job.class);
    }

    public boolean isValid() {
        // TODO Auto-generated method stub
        return false;
    }

}
