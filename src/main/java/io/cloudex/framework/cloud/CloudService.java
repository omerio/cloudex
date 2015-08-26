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

package io.cloudex.framework.cloud;

import java.io.IOException;

/**
 * An interface for cloud API operations. This is generic and is not cloud provider specific. Implementations are needed
 * for the various cloud providers 
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface CloudService {
    
    
    /**
     * Perform any required initialization before
     * this cloud service is used
     * @throws IOException if any of the cloud api calls fail
     */
    public VmMetaData init() throws IOException;
    
    /**
     * Get the meta data of the current instance,
     * Wait for change will block until there is a change
     * @param waitForChange - true to block until there is a change, false to return immediately
     * @return {@link VmMetaData}
     * @throws IOException if any of the cloud api calls fail
     */
    public VmMetaData getMetaData(boolean waitForChange) throws IOException;
    
    /**
     * Get the meta data for the provided instance id
     * @param instanceId - the id of the vm instance
     * @param zoneId - the zone or region id
     * @return {@link VmMetaData}
     * @throws IOException if any of the cloud api calls fail
     */
    public VmMetaData getMetaData(String instanceId, String zoneId) throws IOException;
    
    /**
     * Update the meta data of the current instance
     * @param metaData {@link VmMetaData}
     * @throws IOException if any of the cloud api calls fail
     */
    public void updateMetadata(VmMetaData metaData) throws IOException;
    
    /**
     * Update the meta data of the the provided instance
     * @param metaData {@link VmMetaData}
     * @param zoneId - the zone or region id
     * @param instanceId - the id of the vm instance
     * @param block - true to wait for the api call to complete, false to return immediately
     * @throws IOException if any of the cloud api calls fail
     */
    public void updateMetadata(VmMetaData metaData, 
            String zoneId, String instanceId, boolean block) throws IOException;

}
