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

import io.cloudex.framework.cloud.api.Callback;
import io.cloudex.framework.cloud.api.StorageObject;

import java.io.IOException;
import java.util.List;

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
     * Update the meta data of the current instance. 
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
    
    /**
     * Create VM instances, optionally block until all are created. If any fails then the returned flag is false
     * @param configs
     * @param block
     * @return
     * @throws IOException
     */
    public boolean startInstance(List<VmConfig> configs, boolean block) throws IOException;

    /**
     * Delete the VMs provided in this config
     * @param configs
     * @throws IOException
     */
    public void shutdownInstance(List<VmConfig> configs) throws IOException;

    /**
     * Delete the currently running vm, i.e. self terminate
     * @throws IOException
     */
    public void shutdownInstance() throws IOException;

    /**
     * Get the id of the current instance
     * @return
     */
    public String getInstanceId();
      
    /**
     * Create a bucket on the mass cloud storage
     * @param bucket
     * @param location
     * @throws IOException
     */
    public void createCloudStorageBucket(String bucket, String location) throws IOException;

    /**
     * Upload the provided file into cloud storage
     * @param filename
     * @param bucket
     * @param callback
     * @return 
     * @throws IOException
     */
    public StorageObject uploadFileToCloudStorage(String filename, String bucket, Callback callback) throws IOException;
    
    
    /**
     * Upload a file to cloud storage and block until it's uploaded
     * @param filename
     * @param bucket
     * @return 
     * @throws IOException
     */
    public StorageObject uploadFileToCloudStorage(String filename, String bucket) throws IOException;

    /**
     * Download an object from cloud storage to a file
     * @param object
     * @param outFile
     * @param bucket
     * @param callback
     * @throws IOException
     */
    public void downloadObjectFromCloudStorage(String object, String outFile,
            String bucket, Callback callback) throws IOException;
    
    /**
     * Download an object from cloud storage to a file, this method will block until the file is downloaded
     * @param object
     * @param outFile
     * @param bucket
     * @throws IOException
     */
    public void downloadObjectFromCloudStorage(String object, String outFile,
            String bucket) throws IOException;
    
    /**
     * List all the objects in the provided cloud storage bucket
     * @param bucket
     * @return
     * @throws IOException
     */
    public List<StorageObject> listCloudStorageObjects(String bucket) throws IOException;
    
    /**
     * Return the maximum metadata size for the underlying cloud provider implementation
     * @return maximum metadata size
     */
    public int getMaximumMetaDataSize();
    
    /**
     * Get the sleep period in seconds when waiting for async API operations to complete
     * @return API recheck delay
     */
    public int getApiRecheckDelay();

}
