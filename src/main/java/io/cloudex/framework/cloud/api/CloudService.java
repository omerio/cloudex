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

package io.cloudex.framework.cloud.api;

import io.cloudex.framework.cloud.entities.BigDataTable;
import io.cloudex.framework.cloud.entities.QueryStats;
import io.cloudex.framework.cloud.entities.StorageObject;
import io.cloudex.framework.cloud.entities.VmMetaData;
import io.cloudex.framework.config.VmConfig;

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
    public String updateMetadata(VmMetaData metaData, 
            String zoneId, String instanceId, boolean block) throws IOException;
    
    /**
     * Block and wait the operations with the provided references to complete
     * @param references - the operations references
     * @param zoneId  - the zone or region id
     * @throws IOException if any of the cloud api calls fail
     */
    public void blockOnComputeOperations(List<String> references, String zoneId) throws IOException;

    /**
     * Create VM instances, optionally block until all are created. If any fails then the returned flag is false
     * @param configs - the {@link VmConfig} of the instances to start
     * @param block - true to wait for all the instance to start up
     * @return true if the operation is successful
     * @throws IOException if any of the cloud api calls fail
     */
    public boolean startInstance(List<VmConfig> configs, boolean block) throws IOException;

    /**
     * Delete the VMs provided in this config
     * @param configs - the {@link VmConfig} of the instances to shutdown
     * @throws IOException if any of the cloud api calls fail
     */
    public void shutdownInstance(List<VmConfig> configs) throws IOException;

    /**
     * Delete the currently running vm, i.e. self terminate
     * @throws IOException if any of the cloud api calls fail
     */
    public void shutdownInstance() throws IOException;

    /**
     * Get the id of the current instance
     * @return the id of the current instance
     */
    public String getInstanceId();

    // ----- Cloud Storage service methods
    
    /**
     * Create a bucket on the mass cloud storage
     * @param bucket - the cloud storage bucket
     * @param location - the location where the bucket should be created
     * @throws IOException if any of the cloud api calls fail
     */
    public void createCloudStorageBucket(String bucket, String location) throws IOException;

    /**
     * Upload the provided file into cloud storage
     * @param filename - the name of the file to upload
     * @param bucket - the cloud bucket
     * @param callback - an optional callback when the upload is done
     * @return  a {@link StorageObject}
     * @throws IOException if any of the cloud api calls fail
     */
    public StorageObject uploadFileToCloudStorage(String filename, String bucket, Callback callback) throws IOException;


    /**
     * Upload a file to cloud storage and block until it's uploaded
     * @param filename - the name of the file to upload
     * @param bucket - the cloud storage bucket
     * @return a {@link StorageObject} of the uploaded file
     * @throws IOException if any of the cloud api calls fail
     */
    public StorageObject uploadFileToCloudStorage(String filename, String bucket) throws IOException;

    /**
     * Download an object from cloud storage to a file
     * @param object - the name of the object to download
     * @param outFile - the local file to save the object
     * @param bucket - the cloud storage bucket
     * @param callback - call when the operation completes
     * @throws IOException if any of the cloud api calls fail
     */
    public void downloadObjectFromCloudStorage(String object, String outFile,
            String bucket, Callback callback) throws IOException;

    /**
     * Download an object from cloud storage to a file, this method will block until the file is downloaded
     * @param object - the name of the object to download
     * @param outFile - the local file to save the object
     * @param bucket - the cloud storage bucket
     * @throws IOException if any of the cloud api calls fail
     */
    public void downloadObjectFromCloudStorage(String object, String outFile,
            String bucket) throws IOException;

    /**
     * List all the objects in the provided cloud storage bucket
     * @param bucket - the cloud storage bucket
     * @return a list of {@link StorageObject}
     * @throws IOException if any of the cloud api calls fail
     */
    public List<StorageObject> listCloudStorageObjects(String bucket) throws IOException;

    // ----- Big Data service methods
    
    /*
     * Load a list of cloud storage files into a big data table
     * @param files - The source URIs must be fully-qualified, in the format gs://<bucket>/<object>.
     * @param table
     * @param createTable
     * @return
     * @throws IOException if any of the cloud api calls fail
     */
    public String loadCloudStorageFilesIntoBigData(List<String> files, BigDataTable table, boolean createTable) 
            throws IOException;

    /**
     * Load a list of local files into a big data table
     * @param files - the names of the local files to upload to big data
     * @param table - the table definition
     * @param createTable - true if a new table should be created if it doesn't exist
     * @return a {@link java.util.List} of all the job ids
     * @throws IOException if any of the cloud api calls fail
     */
    public List<String> loadLocalFilesIntoBigData(List<String> files, BigDataTable table, boolean createTable) 
            throws IOException;

    /**
     * Creates an asynchronous Query Job for a particular query on a dataset
     *
     * @param querySql  the actual query string
     * @return a reference to the inserted query job
     * @throws IOException if any of the cloud api calls fail
     */
    public String startBigDataQuery(String querySql) throws IOException;

    /**
     * Polls a big data job and once done save the results to a file
     * @param jobId - the id of a previous big data job
     * @param filename - the destination filename
     * @throws IOException if any of the cloud api calls fail
     */
    public QueryStats saveBigQueryResultsToFile(String jobId, String filename) throws IOException;
    
    // ----- Generic cloud provider specific configurations
    
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
    
    /**
     * Usually this cloud service is used in a cloud VM environment. If remote is set to true, then
     * it's assumed that this service is not run on a VM environment. Useful for local testing of cloud
     * services
     * @param remote - true if this instance is not running on a VM
     */
    public void setRemote(boolean remote);
    
    /**
     * The authentication provider used by this instance
     * @param <T> the class of the authentication object returned by the provider
     * @param provider the authentication provider to use
     * 
     */
    public <T> void setAuthenticationProvider(AuthenticationProvider<T> provider);


}
