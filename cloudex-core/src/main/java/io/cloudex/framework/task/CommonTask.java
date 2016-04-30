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

package io.cloudex.framework.task;

import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.cloud.entities.VmMetaData;

import java.util.HashMap;
import java.util.Map;


/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public abstract class CommonTask implements Task {


    // The task output data
    protected Map<String, Object> output = new HashMap<>();

    // The cloud service
    protected CloudService cloudService;


    /* (non-Javadoc)
     * @see io.cloudex.framework.task.Task#getOutput()
     */
    @Override
    public Map<String, Object> getOutput() {
        return this.output;
    }

    /**
     * Add a key/value to the output of this task. This is a delegate method for Map.put
     * @param key - the key 
     * @param value - the value
     * @return the object that has been added
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    public Object addOutput(String key, Object value) {
        return output.put(key, value);
    }

    /* (non-Javadoc)
     * @see io.cloudex.framework.Executable#setCloudService(io.cloudex.framework.cloud.CloudService)
     */
    @Override
    public void setCloudService(CloudService cloudService) {
        this.cloudService = cloudService;
    }

    /* (non-Javadoc)
     * @see io.cloudex.framework.Executable#getCloudService()
     */
    @Override
    public CloudService getCloudService() {
        return cloudService;
    }

    // metaData is not supported for tasks
    /* (non-Javadoc)
     * @see io.cloudex.framework.CommonExecutable#getMetaData()
     */
    @Override
    public VmMetaData getMetaData() {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see io.cloudex.framework.CommonExecutable#setMetaData(io.cloudex.framework.cloud.MetaData)
     */
    @Override
    public void setMetaData(VmMetaData metaData) {
        throw new UnsupportedOperationException();
    }

}
