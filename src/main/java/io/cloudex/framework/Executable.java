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

package io.cloudex.framework;

import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.cloud.entities.VmMetaData;

import java.io.IOException;

/**
 * Represents a component that can be executed on a cloud virtual machine.
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface Executable {

    /**
     * Run this executable.
     * @throws IOException
     */
    public void run() throws IOException; 


    /**
     * Set the executable cloud service.
     */
    public void setCloudService(CloudService cloudService) throws IOException;

    /**
     * Return the executable cloud service.
     * @return cloudService
     */
    public CloudService getCloudService();

    /**
     * Set the executable metadata.
     * @param metaData
     */
    public void setMetaData(VmMetaData metaData);

    /**
     * Get the executable metadata.
     * @return metaData
     */
    public VmMetaData getMetaData();



}
