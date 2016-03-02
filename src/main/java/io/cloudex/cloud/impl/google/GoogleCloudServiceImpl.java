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
package io.cloudex.cloud.impl.google;

import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.ATTRIBUTES;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.ATTRIBUTES_PATH;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.CLOUD_STORAGE_PREFIX;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.CREATE_IF_NEEDED;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.CREATE_NEVER;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.DATASTORE_SCOPE;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.DEFAULT;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.DISK_TYPES;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.DONE;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.EMAIL;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.EXT_NAT;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.HOSTNAME;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.INSTANCE_ALL_PATH;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.MACHINE_TYPES;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.MIGRATE;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.NETWORK;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.NOT_FOUND;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.ONE_TO_ONE_NAT;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.PERSISTENT;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.PROJECT_ID_PATH;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.RESOURCE_BASE_URL;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.SCOPES;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.SERVICE_ACCOUNTS;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.WAIT_FOR_CHANGE;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.WILDCARD_SUFFIX;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.WRITE_APPEND;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.ZONE;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.ZONES;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import io.cloudex.cloud.impl.google.bigquery.BigQueryStreamable;
import io.cloudex.cloud.impl.google.compute.GoogleMetaData;
import io.cloudex.cloud.impl.google.compute.InstanceStatus;
import io.cloudex.cloud.impl.google.storage.DownloadProgressListener;
import io.cloudex.cloud.impl.google.storage.UploadProgressListener;
import io.cloudex.framework.cloud.api.ApiUtils;
import io.cloudex.framework.cloud.api.AuthenticationProvider;
import io.cloudex.framework.cloud.api.Callback;
import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.cloud.api.FutureTask;
import io.cloudex.framework.cloud.entities.BigDataColumn;
import io.cloudex.framework.cloud.entities.BigDataTable;
import io.cloudex.framework.cloud.entities.QueryStats;
import io.cloudex.framework.cloud.entities.VmMetaData;
import io.cloudex.framework.config.VmConfig;
import io.cloudex.framework.utils.Constants;
import io.cloudex.framework.utils.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Data;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.ExplainQueryStage;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.JobStatistics3;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.Compute.Instances.Insert;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.AttachedDisk;
import com.google.api.services.compute.model.AttachedDiskInitializeParams;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.Metadata.Items;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.Operation.Error.Errors;
import com.google.api.services.compute.model.Scheduling;
import com.google.api.services.compute.model.ServiceAccount;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * A Google Cloud Platform specific implementation of cloudex {@link CloudService}
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class GoogleCloudServiceImpl implements GoogleCloudService {

    private final static Log log = LogFactory.getLog(GoogleCloudServiceImpl.class);

    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    // the maximum size in bytes for metadata
    private int maximumMetaDataSize = 32_768;

    // the delay in seconds between API checks
    private int apiRecheckDelay = 2;

    private int maxBigQueryRequestSize = 100_000;

    private int bigQueryStreamDelay = 2;

    private int bigQueryStreamFailRetries = 3;

    /** Global instance of the HTTP transport. */
    private HttpTransport httpTransport;

    private Storage storage;

    private Compute compute;

    private Bigquery bigquery;

    private String projectId;

    private String zone;

    private String instanceId;

    private String serviceAccount;

    private List<String> scopes;
    
    private boolean remote;
    
    @SuppressWarnings("rawtypes")
    private AuthenticationProvider authenticationProvider;

    /**
     * Perform initialization before
     * this cloud service is used
     * @throws IOException 
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public VmMetaData init() throws IOException {
        
        // can't do anything without an authentication provider
        Validate.notNull(this.authenticationProvider);
        
        Map<String, Object> attributes = null;
        String fingerprint = null;
        
        this.getHttpTransport();
        
        if(this.remote) {
            // if not running on a vm on the cloud environment, then 
            // these values need to be set externally
            Validate.notNull(this.zone);
            Validate.notNull(this.projectId);
            Validate.notNull(this.instanceId);
            Validate.notNull(this.scopes);
            
            Credential credential = (Credential) this.authenticationProvider.authorize();
            
            this.getCompute(credential);
            this.getStorage(credential);
            this.getBigquery(credential);
            
        } else {
            this.projectId = GoogleMetaData.getMetaData(PROJECT_ID_PATH);
            Map<String, Object> metaData = GoogleMetaData.getMetaDataAsMap(INSTANCE_ALL_PATH);
            attributes = (Map<String, Object>) metaData.get(ATTRIBUTES);

            // strangely zone looks like this: "projects/315344313954/zones/us-central1-a"
            this.zone = (String) metaData.get(ZONE);
            this.zone = StringUtils.substringAfterLast(this.zone, "/");

            // the name isn't returned!, but the hostname looks like this:
            // "ecarf-evm-1.c.ecarf-1000.internal"
            this.instanceId = (String) metaData.get(HOSTNAME);
            this.instanceId = StringUtils.substringBefore(this.instanceId, ".");

            // get the default service account
            Map<String, Object> serviceAccountConfig = ((Map) ((Map) metaData.get(SERVICE_ACCOUNTS)).get(DEFAULT));
            this.serviceAccount = (String) serviceAccountConfig.get(EMAIL);
            this.scopes = (List) serviceAccountConfig.get(SCOPES);
            // add the datastore scope as well
            this.scopes.add(DATASTORE_SCOPE);

            // no need for this call right now
            //this.authorise();
            this.getCompute();
            this.getStorage();
            this.getBigquery();

            boolean retrying = true;
            int retries = 0;
            Instance instance = null;
            
            do {
                try {
                    // java.net.SocketTimeoutException: connect timed out
                    instance = this.getInstance(instanceId, zone);
                    retrying = false;
                    
                } catch(IOException e) {
                    log.error("Failed to retrieve instance details, retries: " + retries, e);
                    retries++;
                    if(retries > 3) {
                        throw e;
                    }
                    ApiUtils.block(this.getApiRecheckDelay());
                }
                
            } while(retrying);
            
            fingerprint = instance.getMetadata().getFingerprint();
        }

        log.debug("Successfully initialized Google Cloud Service: " + this);
        return new VmMetaData(attributes, fingerprint);

    }



    /**
     * Create a new instance of the HTTP transport
     * @return
     * @throws GeneralSecurityException
     * @throws IOException
     */
    protected HttpTransport getHttpTransport() throws IOException {
        if(httpTransport == null) {
            try {
                httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            } catch (GeneralSecurityException e) {
                log.error("failed to create transport", e);
                throw new IOException(e);
            }
        }

        return httpTransport;
    }

    /**
     * Get the token and also check if it's expired then request a new one
     * Note: this is only relevant if remote = false
     * @return
     * @throws IOException
     */
    protected String getOAuthToken() throws IOException {
        
        String token = null;
        
        if(!remote) {
            token = (String) this.authenticationProvider.authorize();
        }
        
        return token;
    }
    
    /**
     * Create a compute API client instance
     * @return
     * @throws IOException 
     * @throws GeneralSecurityException 
     */
    protected Compute getCompute() throws IOException {
        return this.getCompute(null);
    }

    /**
     * Create a compute API client instance
     * @return
     * @throws IOException 
     * @throws GeneralSecurityException 
     */
    protected Compute getCompute(Credential credential) throws IOException {
        if(this.compute == null) {
            this.compute = new Compute.Builder(getHttpTransport(), JSON_FACTORY, credential)
                .setApplicationName(Constants.APP_NAME).build();
        }
        return this.compute;
    }
    
    /**
     * Create a bigquery API client instance
     * @return
     * @throws IOException 
     * @throws GeneralSecurityException 
     */
    protected Bigquery getBigquery() throws IOException {
        return this.getBigquery(null);
    }

    /**
     * Create a bigquery API client instance
     * @return
     * @throws IOException 
     * @throws GeneralSecurityException 
     */
    protected Bigquery getBigquery(Credential credential) throws IOException {
        if(this.bigquery == null) {
            this.bigquery = new Bigquery.Builder(getHttpTransport(), JSON_FACTORY, credential)
                .setApplicationName(Constants.APP_NAME).build();
        }
        return this.bigquery;
    }

    /**
     * Create a storage API client instance
     * @return
     * @throws IOException 
     * @throws GeneralSecurityException 
     */
    protected Storage getStorage() throws IOException {
        return this.getStorage(null);
    }
    
    /**
     * Create a storage API client instance
     * @return
     * @throws IOException 
     * @throws GeneralSecurityException 
     */
    protected Storage getStorage(Credential credential) throws IOException {
        if(this.storage == null) {
            this.storage = new  Storage.Builder(getHttpTransport(), JSON_FACTORY, credential)
                .setApplicationName(Constants.APP_NAME).build();
        }
        return this.storage;
    }

    //------------------------------------------------- Storage -------------------------------
    /**
     * Create a bucket on the mass cloud storage
     * @param bucket
     * @throws IOException 
     */
    @Override
    public void createCloudStorageBucket(String bucket, String location) throws IOException {

        Storage.Buckets.Insert insertBucket = this.getStorage().buckets()
                .insert(this.projectId, new Bucket().setName(bucket).setLocation(location)
                        // .setDefaultObjectAcl(ImmutableList.of(
                        // new ObjectAccessControl().setEntity("allAuthenticatedUsers").setRole("READER")))
                        ).setOauthToken(this.getOAuthToken());
        try {
            @SuppressWarnings("unused")
            Bucket createdBucket = insertBucket.execute();
        } catch (GoogleJsonResponseException e) {
            GoogleJsonError error = e.getDetails();
            if (error.getCode() == HTTP_CONFLICT
                    && error.getMessage().contains("You already own this bucket.")) {
                log.debug(bucket + " already exists!");
            } else {
                throw e;
            }
        }
    }

    /**
     * Upload the provided file into cloud storage
     * THis is what Google returns:
     * CONFIG: {
		 "kind": "storage#object",
		 "id": "ecarf/umbel_links.nt_out.gz/1397227987451000",
		 "selfLink": "https://www.googleapis.com/storage/v1beta2/b/ecarf/o/umbel_links.nt_out.gz",
		 "name": "umbel_links.nt_out.gz",
		 "bucket": "ecarf",
		 "generation": "1397227987451000",
		 "metageneration": "1",
		 "contentType": "application/x-gzip",
		 "updated": "2014-04-11T14:53:07.339Z",
		 "storageClass": "STANDARD",
		 "size": "8474390",
		 "md5Hash": "UPhXcZZGbD9198OhQcdnvQ==",
		 "mediaLink": "https://www.googleapis.com/storage/v1beta2/b/ecarf/o/umbel_links.nt_out.gz?generation=1397227987451000&alt=media",
		 "owner": {
		  "entity": "user-00b4903a97e56638621f0643dc282444442a11b19141d3c7b425c4d17895dcf6",
		  "entityId": "00b4903a97e56638621f0643dc282444442a11b19141d3c7b425c4d17895dcf6"
		 },
		 "crc32c": "3twYkA==",
		 "etag": "CPj48u7X2L0CEAE="
		}
     * @param filename
     * @param bucket
     * @throws IOException 
     */
    @Override
    public io.cloudex.framework.cloud.entities.StorageObject uploadFileToCloudStorage(String filename, String bucket, Callback callback) throws IOException {

        FileInputStream fileStream = new FileInputStream(filename);
        String contentType;
        boolean gzipDisabled;

        if(GzipUtils.isCompressedFilename(filename)) {
            contentType = Constants.GZIP_CONTENT_TYPE;
            gzipDisabled = true;

        } else {
            contentType = Files.probeContentType((new File(filename)).toPath());
            if(contentType == null) {
                contentType = Constants.BINARY_CONTENT_TYPE;
            }
            gzipDisabled = false;
        }

        InputStreamContent mediaContent = new InputStreamContent(contentType, fileStream);

        // Not strictly necessary, but allows optimization in the cloud.
        mediaContent.setLength(fileStream.available());

        Storage.Objects.Insert insertObject =
                getStorage().objects().insert(bucket, 
                        new StorageObject().setName(StringUtils.substringAfterLast(filename, FileUtils.PATH_SEPARATOR)), 
                        mediaContent).setOauthToken(this.getOAuthToken());

        insertObject.getMediaHttpUploader().setProgressListener(
                new UploadProgressListener(callback)).setDisableGZipContent(gzipDisabled);
        // For small files, you may wish to call setDirectUploadEnabled(true), to
        // reduce the number of HTTP requests made to the server.
        if (mediaContent.getLength() > 0 && mediaContent.getLength() <=  4 * FileUtils.ONE_MB /* 2MB */) {
            insertObject.getMediaHttpUploader().setDirectUploadEnabled(true);
        }

        StorageObject object = null;
        
        try {
            object = insertObject.execute();
        
        } catch(IOException e) {
            
            log.error("Error whilst uploading file", e);
            
            ApiUtils.block(5);
            // try again
            object = insertObject.execute();
        }

        return this.getCloudExStorageObject(object, bucket);
    }

    /**
     * Upload a file to cloud storage and block until it's uploaded
     * @param filename
     * @param bucket
     * @throws IOException
     */
    @Override
    public io.cloudex.framework.cloud.entities.StorageObject uploadFileToCloudStorage(String filename, String bucket) throws IOException {

        final FutureTask task = new FutureTask();

        Callback callback = new Callback() {
            @Override
            public void execute() {
                log.debug("Upload complete");
                task.setDone(true);
            }
        };

        io.cloudex.framework.cloud.entities.StorageObject storageObject = this.uploadFileToCloudStorage(filename, bucket, callback);

        // wait for the upload to finish
        while(!task.isDone()) {
            ApiUtils.block(5);
        }

        return storageObject;

    }

    /**
     * Download an object from cloud storage to a file
     * @param object
     * @param outFile
     * @param bucket
     * @param callback
     * @throws IOException
     */
    @Override
    public void downloadObjectFromCloudStorage(String object, String outFile, 
            String bucket, Callback callback) throws IOException {

        log.debug("Downloading cloud storage file " + object + ", to: " + outFile);

        FileOutputStream out = new FileOutputStream(outFile);

        Storage.Objects.Get getObject =
                getStorage().objects().get(bucket, object).setOauthToken(this.getOAuthToken());

        getObject.getMediaHttpDownloader().setDirectDownloadEnabled(true)
        .setProgressListener(new DownloadProgressListener(callback));

        getObject.executeMediaAndDownloadTo(out);

    }

    /**
     * Download an object from cloud storage to a file, this method will block until the file is downloaded
     * @param object
     * @param outFile
     * @param bucket
     * @param callback
     * @throws IOException
     * TODO Parallel/Multi download
     */
    @Override
    public void downloadObjectFromCloudStorage(String object, final String outFile, 
            String bucket) throws IOException {

        //final Thread currentThread = Thread.currentThread();
        final FutureTask task = new FutureTask();

        Callback callback = new Callback() {
            @Override
            public void execute() {
                log.debug("Download complete, file saved to: " + outFile);
                //LockSupport.unpark(currentThread);
                task.setDone(true);
            }
        };

        this.downloadObjectFromCloudStorage(object, outFile, bucket, callback);

        // wait for the download to take place
        //LockSupport.park();
        while(!task.isDone()) {
            ApiUtils.block(5);
        }

    }

    @Override
    public List<io.cloudex.framework.cloud.entities.StorageObject> listCloudStorageObjects(String bucket) throws IOException {
        List<io.cloudex.framework.cloud.entities.StorageObject> objects = new ArrayList<>();
        Storage.Objects.List listObjects =  
                getStorage().objects().list(bucket).setOauthToken(this.getOAuthToken());
        // we are not paging, just get everything
        Objects cloudObjects = listObjects.execute();

        for (StorageObject cloudObject : cloudObjects.getItems()) {
            // Do things!
            io.cloudex.framework.cloud.entities.StorageObject object = this.getCloudExStorageObject(cloudObject, bucket);
            objects.add(object);
        }
        return objects;
    }

    /**
     * Create an ecarf storage object from a Google com.google.api.services.storage.model.StorageObject
     * @param cloudObject
     * @param bucket
     * @return
     */
    private io.cloudex.framework.cloud.entities.StorageObject getCloudExStorageObject(StorageObject cloudObject, String bucket) {
        io.cloudex.framework.cloud.entities.StorageObject object = 
                new io.cloudex.framework.cloud.entities.StorageObject();
        object.setContentType(cloudObject.getContentType());
        object.setDirectLink(cloudObject.getSelfLink());
        object.setName(cloudObject.getName());
        object.setSize(cloudObject.getSize());
        object.setUri(CLOUD_STORAGE_PREFIX + bucket + "/" + cloudObject.getName());
        return object;
    }

    
    //------------------------------------------------- Compute -------------------------------

    /**
     * 
     * @param instanceId
     * @param zoneId
     * @return
     * @throws IOException
     */
    private Instance getInstance(String instanceId, String zoneId) throws IOException {
        return this.getCompute().instances().get(this.projectId, 
                zoneId != null ? zoneId : this.zone, instanceId)
                .setOauthToken(this.getOAuthToken())
                .execute();
    }


    /**
     * Get the meta data of the current instance, this will simply call the metadata server.
     * Wait for change will block until there is a change
     * @return
     * @throws IOException 
     */
    @Override
    public VmMetaData getMetaData(boolean waitForChange) throws IOException {

        Map<String, Object> attributes = GoogleMetaData.getMetaDataAsMap(
                ATTRIBUTES_PATH + (waitForChange ? WAIT_FOR_CHANGE : ""));
        Instance instance = this.getInstance(instanceId, zone);
        String fingerprint = instance.getMetadata().getFingerprint();
        return new VmMetaData(attributes, fingerprint);

    }

    /**
     * Get the meta data for the provided instance id
     * @param instanceId
     * @param zoneId
     * @return
     * @throws IOException 
     */
    @Override
    public VmMetaData getMetaData(String instanceId, String zoneId) throws IOException {
        Instance instance = this.getInstance(instanceId != null ? instanceId : this.instanceId, 
                zoneId != null ? zoneId : this.zone);
        List<Items> items = instance.getMetadata().getItems();
        Map<String, Object> attributes = new HashMap<>();
        for(Items item: items) {
            attributes.put(item.getKey(), item.getValue());
        }
        String fingerprint = instance.getMetadata().getFingerprint();
        return new VmMetaData(attributes, fingerprint);
    }

    /**
     *
     * Update the meta data of the current instance
     * @param key
     * @param value
     * @throws IOException
     */
    @Override
    public void updateMetadata(VmMetaData metaData) throws IOException {
        this.updateMetadata(metaData, this.zone, this.instanceId, true);
    }

    /**
     * 
     * Update the meta data of the the provided instance
     * @param key
     * @param value
     * @throws IOException
     */
    @Override
    public String updateMetadata(VmMetaData metaData, 
            String zoneId, String instanceId, boolean block) throws IOException {

        Metadata metadata = this.getGoogleMetaData(metaData);

        Operation operation = this.getCompute().instances().setMetadata(projectId, zoneId, instanceId, metadata)
                .setOauthToken(this.getOAuthToken()).execute();

        log.debug("Successuflly initiated operation: " + operation);

        // shall we wait until the operation is complete?
        if(block) {
            this.blockOnOperation(operation, zoneId);

            // update the fingerprint of the current metadata
            Instance instance = this.getInstance(instanceId, zoneId);
            metaData.setFingerprint(instance.getMetadata().getFingerprint());
        
        }
        
        return operation.getName();
    }
    
    /**
     * Block and wait the operations with the provided references to complete
     * @param references
     * @param zoneId
     * @throws IOException
     */
    @Override
    public void blockOnComputeOperations(List<String> references, String zoneId) throws IOException {

        for(String reference: references) {
            
            Operation operation = null;

            do {
                
                 operation = this.getCompute().zoneOperations().get(projectId, zoneId, reference)
                        .setOauthToken(this.getOAuthToken()).execute();


                // check if the operation has actually failed
                if((operation.getError() != null) && (operation.getError().getErrors() != null)) {
                    Errors error = operation.getError().getErrors().get(0);
                    throw new IOException("Operation failed: " + error.getCode() + " - " + error.getMessage());
                }
                
            } while((operation != null) && !DONE.endsWith(operation.getStatus()));
        }
    }

    /**
     * Wait until the provided operation is done, if the operation returns an error then an IOException 
     * will be thrown
     * @param operation
     * @param zoneId
     * @throws IOException
     */
    protected void blockOnOperation(Operation operation, String zoneId) throws IOException {
        do {
            // sleep for 2 seconds before checking the operation status
            ApiUtils.block(this.getApiRecheckDelay());

            operation = this.getCompute().zoneOperations().get(projectId, zoneId, operation.getName())
                    .setOauthToken(this.getOAuthToken()).execute();

            // check if the operation has actually failed
            if((operation.getError() != null) && (operation.getError().getErrors() != null)) {
                Errors error = operation.getError().getErrors().get(0);
                throw new IOException("Operation failed: " + error.getCode() + " - " + error.getMessage());
            }

        } while(!DONE.endsWith(operation.getStatus()));
    }

    /**
     * Create VM instances, optionally block until all are created. If any fails then the returned flag is false
     * 
     *  body = {
    'name': NEW_INSTANCE_NAME,
    'machineType': <fully-qualified-machine-type-url>,
    'networkInterfaces': [{
      'accessConfigs': [{
        'type': 'ONE_TO_ONE_NAT',
        'name': 'External NAT'
       }],
      'network': <fully-qualified-network-url>
    }],
    'disk': [{
       'autoDelete': 'true',
       'boot': 'true',
       'type': 'PERSISTENT',
       'initializeParams': {
          'diskName': 'my-root-disk',
          'sourceImage': '<fully-qualified-image-url>',
       }
     }]
  }
     * @param config
     * @param block
     * @throws IOException
     * TODO start VMs in parallel
     */
    @Override
    public boolean startInstance(List<VmConfig> configs, boolean block) throws IOException {

        for(VmConfig config: configs) {
            log.debug("Creating VM for config: " + config);
            String zoneId = config.getZoneId();
            zoneId = zoneId != null ? zoneId : this.zone;
            Instance content = new Instance();

            // Instance config
            content.setMachineType(RESOURCE_BASE_URL + 
                    this.projectId + ZONES + zoneId + MACHINE_TYPES+ config.getVmType());
            content.setName(config.getInstanceId());
            //content.setZone(zoneId);
            // startup script
            if(StringUtils.isNoneBlank(config.getStartupScript())) {
                config.getMetaData().addValue(GoogleMetaData.STARTUP_SCRIPT, config.getStartupScript());
            }
            content.setMetadata(this.getGoogleMetaData(config.getMetaData()));

            // service account
            ServiceAccount sa = new ServiceAccount();
            sa.setEmail(this.serviceAccount).setScopes(this.scopes);
            content.setServiceAccounts(Lists.newArrayList(sa));

            // network
            NetworkInterface inf = new NetworkInterface(); 
            inf.setNetwork(RESOURCE_BASE_URL + 
                    this.projectId + NETWORK + config.getNetworkId());
            AccessConfig accessConf = new AccessConfig();
            accessConf.setType(ONE_TO_ONE_NAT).setName(EXT_NAT);
            inf.setAccessConfigs(Lists.newArrayList(accessConf));
            content.setNetworkInterfaces(Lists.newArrayList(inf));

            // scheduling
            Scheduling scheduling = new Scheduling();
            scheduling.setAutomaticRestart(false);
            scheduling.setOnHostMaintenance(MIGRATE);
            content.setScheduling(scheduling);

            // Disk
            AttachedDisk disk = new AttachedDisk();

            AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();
            params.setDiskName(config.getInstanceId());
            params.setSourceImage(RESOURCE_BASE_URL + config.getImageId());
            if(config.getDiskSize() != null) {
                params.setDiskSizeGb(config.getDiskSize());
            }

            disk.setAutoDelete(true).setBoot(true)
            .setDeviceName(config.getInstanceId())
            .setType(PERSISTENT)
            .setInitializeParams(params);

            if(StringUtils.isNotBlank(config.getDiskType())) {
                // standard or SSD based disks
                params.setDiskType(RESOURCE_BASE_URL + 
                        this.projectId + ZONES + zoneId + DISK_TYPES + config.getDiskType());
            }

            content.setDisks(Lists.newArrayList(disk));

            Insert insert = this.getCompute().instances()
                    .insert(this.projectId, zoneId, content)
                    .setOauthToken(this.getOAuthToken());

            Operation operation = insert.execute();
            log.debug("Successuflly initiated operation: " + operation);
        }

        boolean success = true;

        // we have to block until all instances are provisioned
        if(block) {

            for(VmConfig config: configs) {
                String status = InstanceStatus.PROVISIONING.toString();

                int retries = 10;

                do {

                    // sleep for some seconds before checking the vm status
                    ApiUtils.block(this.getApiRecheckDelay());

                    String zoneId = config.getZoneId();
                    zoneId = zoneId != null ? zoneId : this.zone;
                    // check the instance status
                    Instance instance = null;

                    // seems the Api sometimes return a not found exception
                    try {
                        instance = this.getInstance(config.getInstanceId(), zoneId);
                        status = instance.getStatus();
                        log.debug(config.getInstanceId() + ", current status is: " + status);

                    } catch(GoogleJsonResponseException e) {
                        if(e.getMessage().indexOf(NOT_FOUND) == 0) {
                            log.warn("Instance not found: " + config.getInstanceId());
                            if(retries <= 0) {
                                throw e;
                            }
                            retries--;

                        } else {
                            throw e;
                        }
                    }
                    // FIXME INFO: ecarf-evm-1422261030407, current status is: null
                } while (InstanceStatus.IN_PROGRESS.contains(status));

                if(InstanceStatus.TERMINATED.equals(status)) {
                    success = false;
                }
            }
        }

        return success;
    }

    /**
     * Delete the VMs provided in this config
     * @param configs
     * @throws IOException 
     *
     */
    @Override
    public void shutdownInstance(List<VmConfig> configs) throws IOException {
        for(VmConfig config: configs) {
            log.debug("Deleting VM: " + config);
            String zoneId = config.getZoneId();
            zoneId = zoneId != null ? zoneId : this.zone;
            this.getCompute().instances().delete(this.projectId, zoneId, config.getInstanceId())
            .setOauthToken(this.getOAuthToken()).execute();
        }
    }

    /**
     * Delete the currently running vm, i.e. self terminate
     * @throws IOException 
     */
    @Override
    public void shutdownInstance() throws IOException {

        log.debug("Deleting VM: " + this.instanceId);
        this.getCompute().instances().delete(this.projectId, this.zone, this.instanceId)
        .setOauthToken(this.getOAuthToken()).execute();

    }

    /**
     * Create an API Metadata
     * @param vmMetaData
     * @return
     */
    protected Metadata getGoogleMetaData(VmMetaData vmMetaData) {
        Metadata metadata = new Metadata();

        Items item;
        List<Items> items = new ArrayList<>();
        for(Entry<String, Object> entry: vmMetaData.getAttributes().entrySet()) {
            item = new Items();
            item.setKey(entry.getKey()).setValue((String) (entry.getValue()));
            items.add(item);
        }
        metadata.setItems(items);
        metadata.setFingerprint(vmMetaData.getFingerprint());

        return metadata;
    }


    //------------------------------------------------- Bigquery -------------------------------


    /**
     * CONFIG: {
		 "kind": "bigquery#job",
		 "etag": "\"QPJfVWBscaHhAhSLq0k5xRS6X5c/xAdd09GSpMDr9PxAk-WGEBWxlKA\"",
		 "id": "ecarf-1000:job_uUL5E0xmOjKxf3hREEZvb5B_M78",
		 "selfLink": "https://www.googleapis.com/bigquery/v2/projects/ecarf-1000/jobs/job_uUL5E0xmOjKxf3hREEZvb5B_M78",
		 "jobReference": {
		  "projectId": "ecarf-1000",
		  "jobId": "job_uUL5E0xmOjKxf3hREEZvb5B_M78"
		 },
		 "configuration": {
		  "load": {
		   "sourceUris": [
		    "gs://ecarf/umbel_links.nt_out.gz",
		    "gs://ecarf/yago_links.nt_out.gz"
		   ],
		   "schema": {
		    "fields": [
		     {
		      "name": "subject",
		      "type": "STRING"
		     },
		     {
		      "name": "object",
		      "type": "STRING"
		     },
		     {
		      "name": "predicate",
		      "type": "STRING"
		     }
		    ]
		   },
		   "destinationTable": {
		    "projectId": "ecarf-1000",
		    "datasetId": "swetodlp",
		    "tableId": "test"
		   }
		  }
		 },
		 "status": {
		  "state": "DONE"
		 },
		 "statistics": {
		  "creationTime": "1398091486326",
		  "startTime": "1398091498083",
		  "endTime": "1398091576483",
		  "load": {
		   "inputFiles": "2",
		   "inputFileBytes": "41510712",
		   "outputRows": "3782729",
		   "outputBytes": "554874551"
		  }
		 }
		}
     * @param files - The source URIs must be fully-qualified, in the format gs://<bucket>/<object>.
     * @param table
     * @return
     * @throws IOException
     */
    @Override
    public String loadCloudStorageFilesIntoBigData(List<String> files, BigDataTable table, boolean createTable) throws IOException {
        log.debug("Loading data from files: " + files + ", into big data table: " + table);

        Validate.notNull(table.getName());

        Job job = new Job();
        JobConfiguration config = new JobConfiguration();
        JobConfigurationLoad load = new JobConfigurationLoad();	
        config.setLoad(load);
        job.setConfiguration(config);

        load.setSourceUris(files);
        load.setCreateDisposition(createTable ? CREATE_IF_NEEDED : CREATE_NEVER);
        load.setWriteDisposition(WRITE_APPEND);

        Validate.notNull(table.getColumns());
        load.setSchema(this.getBigQueryTableSchema(table));

        String [] names = StringUtils.split(table.getName(), '.');
        TableReference tableRef = (new TableReference())
                .setProjectId(this.projectId)
                .setDatasetId(names[0])
                .setTableId(names[1]);
        load.setDestinationTable(tableRef);

        Bigquery.Jobs.Insert insert = this.getBigquery().jobs().insert(projectId, job);

        insert.setProjectId(projectId);
        insert.setOauthToken(this.getOAuthToken());

        JobReference jobRef = insert.execute().getJobReference();

        log.debug("Job ID of Load Job is: " + jobRef.getJobId());

        // TODO add retry support
        return this.checkBigQueryJobResults(jobRef.getJobId(), false, true);

    }

    /**
     * Create a Google BigQuery table schema
     * @param table
     * @return
     */
    protected TableSchema getBigQueryTableSchema(BigDataTable table) {

        TableSchema schema = new TableSchema();

        List<TableFieldSchema> fields = new ArrayList<>();
        TableFieldSchema field;
        List<BigDataColumn> columns = table.getColumns();
        
        for(BigDataColumn column: columns) {
            field = new TableFieldSchema();
            field.setName(column.getName());
            field.setType(column.getType());
            if(column.isRequired()) {
                field.setMode(GoogleMetaData.REQUIRED);
            }
            fields.add(field);
        }
        schema.setFields(fields);

        return schema;

    }


    /**
     * CONFIG: {
	 "kind": "bigquery#job",
	 "etag": "\"QPJfVWBscaHhAhSLq0k5xRS6X5c/RenEm3VqmGyNz-qo48hIw9I6GYQ\"",
	 "id": "ecarf-1000:job_gJed2_eIOXJaMi8RXKjps0hgFhY",
	 "selfLink": "https://www.googleapis.com/bigquery/v2/projects/ecarf-1000/jobs/job_gJed2_eIOXJaMi8RXKjps0hgFhY",
	 "jobReference": {
	  "projectId": "ecarf-1000",
	  "jobId": "job_gJed2_eIOXJaMi8RXKjps0hgFhY"
	 },
	 "configuration": {
	  "load": {
	   "schema": {
	    "fields": [
	     {
	      "name": "subject",
	      "type": "STRING"
	     },
	     {
	      "name": "object",
	      "type": "STRING"
	     },
	     {
	      "name": "predicate",
	      "type": "STRING"
	     }
	    ]
	   },
	   "destinationTable": {
	    "projectId": "ecarf-1000",
	    "datasetId": "swetodlp",
	    "tableId": "test"
	   },
	   "createDisposition": "CREATE_NEVER",
	   "encoding": "UTF-8"
	  }
	 },
	 "status": {
	  "state": "RUNNING"
	 },
	 "statistics": {
	  "creationTime": "1398092776236",
	  "startTime": "1398092822962",
	  "load": {
	   "inputFiles": "1",
	   "inputFileBytes": "8474390"
	  }
	 }
	}
     * @param files
     * @param table
     * @param createTable
     * @return
     * @throws IOException
     */
    @Override
    public List<String> loadLocalFilesIntoBigData(List<String> files, BigDataTable table, boolean createTable) throws IOException {
        /*TableSchema schema = new TableSchema();
        schema.setFields(new ArrayList<TableFieldSchema>());
        JacksonFactory JACKSON = new JacksonFactory();
        JACKSON.createJsonParser(new FileInputStream("schema.json"))
        .parseArrayAndClose(schema.getFields(), TableFieldSchema.class, null);
        schema.setFactory(JACKSON);*/

        Stopwatch stopwatch = Stopwatch.createStarted();

        String [] names = getTableAndDatasetNames(table.getName());
        
        TableReference tableRef = (new TableReference())
                .setProjectId(this.projectId)
                .setDatasetId(names[0])
                .setTableId(names[1]);

        Job job = new Job();
        JobConfiguration config = new JobConfiguration();
        JobConfigurationLoad load = new JobConfigurationLoad();

        Validate.notNull(table.getColumns());
        load.setSchema(this.getBigQueryTableSchema(table));
        load.setDestinationTable(tableRef);

        load.setEncoding(Constants.UTF8);
        load.setCreateDisposition(createTable ? CREATE_IF_NEEDED : CREATE_NEVER);
        load.setWriteDisposition(WRITE_APPEND);

        config.setLoad(load);
        job.setConfiguration(config);

        List<String> jobIds = new ArrayList<>();

        for(String file: files) {
            FileContent content = new FileContent(Constants.BINARY_CONTENT_TYPE, new File(file));

            Bigquery.Jobs.Insert insert = this.getBigquery().jobs().insert(projectId, job, content);

            insert.setProjectId(projectId);
            insert.setOauthToken(this.getOAuthToken());

            JobReference jobRef = insert.execute().getJobReference();
            jobIds.add(jobRef.getJobId());

        }
        List<String> completedIds = new ArrayList<>();

        for(String jobId: jobIds) {
            // TODO add retry support
            completedIds.add(this.checkBigQueryJobResults(jobId, false, false));
        }

        log.debug("Uploaded " + files.size() + " files into bigquery in " + stopwatch );

        return completedIds;

    }
    
    /**
     * Return the name of the table and it's dataset i.e.
     * mydataset.mytable returns [mydataset, mytable]
     * @param table
     * @return
     */
    private String [] getTableAndDatasetNames(String table) {
        Validate.notNull(table);
        
        return StringUtils.split(table, '.');
    }

    /**
     * Stream triple data into big query
     * @param files
     * @param table
     * @param createTable
     * @throws IOException
     */
    @Override
    public void streamObjectsIntoBigData(Collection<? extends BigQueryStreamable> objects, BigDataTable table) throws IOException {

        Stopwatch stopwatch = Stopwatch.createStarted();

        String [] names = getTableAndDatasetNames(table.getName());

        String datasetId = names[0];
        String tableId = names[1];
        //String timestamp = Long.toString((new Date()).getTime());

        List<TableDataInsertAllRequest.Rows>  rowList = new ArrayList<>();

        //TableRow row;
        TableDataInsertAllRequest.Rows rows;

        for(BigQueryStreamable object: objects) {
            //row = new TableRow();
            //row.set
            rows = new TableDataInsertAllRequest.Rows();
            //rows.setInsertId(timestamp);
            rows.setJson(object.toMap());
            rowList.add(rows);
        }

        if(rowList.size() > maxBigQueryRequestSize) {

            int itr = (int) Math.ceil(rowList.size() * 1.0 / maxBigQueryRequestSize);

            for(int i = 1; i <= itr; i++) {

                int index;

                if(i == itr) {
                    // last iteration
                    index = rowList.size();

                } else {
                    index = maxBigQueryRequestSize;
                }

                List<TableDataInsertAllRequest.Rows> requestRows = rowList.subList(0, index);
                this.streamRowsIntoBigQuery(datasetId, tableId, requestRows, 0);
                requestRows.clear();

                //block for a short moment to avoid rateLimitExceeded errors
                ApiUtils.block(bigQueryStreamDelay);

            }

        } else {
            this.streamRowsIntoBigQuery(datasetId, tableId, rowList, 0);
        }

        stopwatch.stop();

        log.debug("Streamed " + objects.size() + " triples into bigquery in " + stopwatch );

    }

    /**
     * Stream a list of rows into bigquery. Retries 3 times if the insert of some rows has failed, i.e. Bigquery returns
     * an insert error
     * 
     *  {
		  "insertErrors" : [ {
		    "errors" : [ {
		      "reason" : "timeout"
		    } ],
		    "index" : 8
		  }],
  		  "kind" : "bigquery#tableDataInsertAllResponse"
  		}
     *	  
     * @param datasetId
     * @param tableId
     * @param rowList
     * @throws IOException
     */
    protected void streamRowsIntoBigQuery(String datasetId, String tableId, 
            List<TableDataInsertAllRequest.Rows>  rowList, int retries) throws IOException {

        /*
         * ExponentialBackOff backoff = ExponentialBackOff.builder()
	    .setInitialIntervalMillis(500)
	    .setMaxElapsedTimeMillis(900000)
	    .setMaxIntervalMillis(6000)
	    .setMultiplier(1.5)
	    .setRandomizationFactor(0.5)
	    .build();
         */

        TableDataInsertAllRequest content = new TableDataInsertAllRequest().setRows(rowList);

        boolean retrying = false;
        ExponentialBackOff backOff = new ExponentialBackOff();

        TableDataInsertAllResponse response = null;
        // keep trying and exponentially backoff as needed
        do {
            try {

                response = this.getBigquery().tabledata().insertAll(
                        this.projectId, datasetId, tableId, content)
                        .setOauthToken(this.getOAuthToken()).execute();

                log.debug(response.toPrettyString());
                retrying = false;

            } catch(GoogleJsonResponseException e) {


                GoogleJsonError error = e.getDetails();	

                // check for rate limit errors
                if((error != null) && (error.getErrors() != null) && !error.getErrors().isEmpty() &&
                        GoogleMetaData.RATE_LIMIT_EXCEEDED.equals(error.getErrors().get(0).getReason())) {

                    log.error("Failed to stream data, error: " + error.getMessage());

                    long backOffTime = backOff.nextBackOffMillis();

                    if (backOffTime == BackOff.STOP) {
                        // we are not retrying anymore
                        log.warn("Failed after " + backOff.getElapsedTimeMillis() / 1000 + " seconds of elapsed time");
                        throw e;

                    } else {
                        int period = (int) Math.ceil(backOffTime / 1000);
                        if(period == 0) {
                            period = 1;
                        }
                        log.debug("Backing off for " + period + " seconds.");
                        ApiUtils.block(period);
                        retrying = true;
                    }

                } else {
                    log.error("Failed to stream data", e);
                    throw e;
                }
            }

        } while(retrying);

        // check for failed rows
        if((response != null) && (response.getInsertErrors() != null) && !response.getInsertErrors().isEmpty()) {
            List<TableDataInsertAllRequest.Rows> failedList = new ArrayList<>();

            List<InsertErrors> insertErrors = response.getInsertErrors();

            for(InsertErrors error: insertErrors) {
                failedList.add(rowList.get(error.getIndex().intValue()));
            }

            // retry again for the failed list
            if(retries > bigQueryStreamFailRetries) {

                log.warn("Failed to stream some rows into bigquery after 3 retries");
                throw new IOException("Failed to stream some rows into bigquery after 3 retries");

            } else {
                retries++;
                log.warn(failedList.size() + " rows failed to be inserted retrying again. Retries = " + retries); 
                this.streamRowsIntoBigQuery(datasetId, tableId, failedList, retries);
            }
        }
    }

    @Override
    public String startBigDataQuery(String querySql) throws IOException {
        
        return this.startBigDataQuery(querySql, null);
        
    }
    
    /**
     * Creates an asynchronous Query Job for a particular query on a dataset
     *
     * @param bigquery  an authorized BigQuery client
     * @param projectId a String containing the project ID
     * @param querySql  the actual query string
     * @return a reference to the inserted query job
     * @throws IOException
     */
    @Override
    public String startBigDataQuery(String querySql, BigDataTable table) throws IOException {

        log.debug("Inserting Query Job: " + querySql);

        Job job = new Job();
        JobConfiguration config = new JobConfiguration();
        JobConfigurationQuery queryConfig = new JobConfigurationQuery();
        config.setQuery(queryConfig);

        job.setConfiguration(config);
        queryConfig.setQuery(querySql);
        
        // if a table is provided then set the large results to true and supply
        // a temp table
        if(table != null) {
            String [] names = this.getTableAndDatasetNames(table.getName());
            String insId = StringUtils.replace(instanceId, "-", "_");
            String tempTable = names[1] + '_' + insId + '_' + System.currentTimeMillis();            
            
            TableReference tableRef = (new TableReference())
                    .setProjectId(this.projectId)
                    .setDatasetId(names[0])
                    .setTableId(tempTable);
            
            queryConfig.setAllowLargeResults(true);
            queryConfig.setDestinationTable(tableRef);
        }

        com.google.api.services.bigquery.Bigquery.Jobs.Insert insert = 
                this.getBigquery().jobs().insert(projectId, job);

        insert.setProjectId(projectId);
        insert.setOauthToken(this.getOAuthToken());
        // TODO java.net.SocketTimeoutException: Read timed out
        JobReference jobRef = insert.execute().getJobReference();

        log.debug("Job ID of Query Job is: " + jobRef.getJobId());

        return jobRef.getJobId();
    }
    
    /**
     * Polls the status of a BigQuery job, returns Job reference if "Done"
     * This method will block until the job status is Done
     * @param jobId     a reference to an inserted query Job
     * @return a reference to the completed Job
     * @throws IOException
     */
    protected String checkBigQueryJobResults(String jobId, boolean retry, boolean throwError) throws IOException {
        
        Job pollJob = this.waitForBigQueryJobResults(jobId, retry, throwError);
        
        return pollJob.getJobReference().getJobId();
    }

    /**
     * Polls the status of a BigQuery job, returns Job reference if "Done"
     * This method will block until the job status is Done
     * @param jobId     a reference to an inserted query Job
     * @return a reference to the completed Job
     * @throws IOException
     */
    protected Job waitForBigQueryJobResults(String jobId, boolean retry, boolean throwError) throws IOException {
        // Variables to keep track of total query time
        Stopwatch stopwatch = Stopwatch.createStarted();

        String status = null;
        Job pollJob = null;
        int retries = 0;
        
        int interval = this.getApiRecheckDelay();

        do {
            
            try {
                pollJob = this.getBigquery().jobs().get(projectId, jobId)
                        .setOauthToken(this.getOAuthToken()).execute();

                status = pollJob.getStatus().getState();

                log.debug("Job Status: " + status + ", elapsed time (secs): " + stopwatch);

                // Pause execution for one second before polling job status again, to
                // reduce unnecessary calls to the BigQUery API and lower overall
                // application bandwidth.
                if (!GoogleMetaData.DONE.equals(status)) {
                    ApiUtils.block(interval);

                    // add a second sleep time for every minute taken by the job
                    // some bigquery jobs can take long time e.g. export
                    int minutes = (int) stopwatch.elapsed(TimeUnit.MINUTES);
                    if(minutes > interval) {
                        interval = minutes;
                    }
                }

            } catch(IOException e) {
                log.error("Failed to get job details, retries" + retries, e);
                retries++;
                if(retries > 3) {
                    throw e;
                }
            }
            //  Error handling

        } while (!GoogleMetaData.DONE.equals(status));

        stopwatch.stop();

        //String completedJobId = pollJob.getJobReference().getJobId();

        log.debug("Job completed successfully" + pollJob.toPrettyString());
        this.printJobStats(pollJob);

        if(retry && (pollJob.getStatus().getErrorResult() != null)) {
            pollJob = this.retryFailedBigQueryJob(pollJob);
        }

        if(throwError && (pollJob.getStatus().getErrorResult() != null)) {
            ErrorProto error = pollJob.getStatus().getErrorResult();
            log.debug("Error result" + error);
            throw new IOException("message: " + error.getMessage() + ", reason: " + error.getReason());
        }

        return pollJob;
    }

    /**
     * Check a number of completed jobs
     * @param jobId
     * @return
     * @throws IOException
     */
    protected Job getCompletedBigQueryJob(String jobId, boolean prettyPrint) throws IOException {

        Job pollJob = this.getBigquery().jobs().get(projectId, jobId)
                .setOauthToken(this.getOAuthToken()).execute();

        String status = pollJob.getStatus().getState();

        if (!GoogleMetaData.DONE.equals(status)) {
            pollJob = null;
            log.warn("Job has not completed yet, skipping. JobId: " + jobId);

        } else {
            if(prettyPrint) {
                log.debug("Job completed successfully" + pollJob.toPrettyString());
            }
            this.printJobStats(pollJob);
        }

        return pollJob;
    }

    /**
     * Retry if a bigquery job has failed due to transient errors
     * {
		  "configuration" : {
		    "query" : {
		      "createDisposition" : "CREATE_IF_NEEDED",
		      "destinationTable" : {
		        "datasetId" : "_f14a24df5a43859914cb508177aa01d64466d055",
		        "projectId" : "ecarf-1000",
		        "tableId" : "anon3fe271d7c2fafca6fbe9e0490a1488c103a3a8fd"
		      },
		      "query" : "select subject,object from [ontologies.swetodblp@-221459-] where predicate=\"<http://lsdis.cs.uga.edu/projects/semdis/opus#last_modified_date>\";",
		      "writeDisposition" : "WRITE_TRUNCATE"
		    }
		  },
		  "etag" : "\"lJkBaCYfTrFXwh5N7-r9owDp5yw/O-f9DO_VlENTr60IoQaXlNb3dFQ\"",
		  "id" : "ecarf-1000:job_QGAraWZPcZLYbm6IhmIyT7aOhmQ",
		  "jobReference" : {
		    "jobId" : "job_QGAraWZPcZLYbm6IhmIyT7aOhmQ",
		    "projectId" : "ecarf-1000"
		  },
		  "kind" : "bigquery#job",
		  "selfLink" : "https://www.googleapis.com/bigquery/v2/projects/ecarf-1000/jobs/job_QGAraWZPcZLYbm6IhmIyT7aOhmQ",
		  "statistics" : {
		    "creationTime" : "1399203726826",
		    "endTime" : "1399203727264",
		    "startTime" : "1399203727120"
		  },
		  "status" : {
		    "errorResult" : {
		      "message" : "Connection error. Please try again.",
		      "reason" : "backendError"
		    },
		    "errors" : [ {
		      "message" : "Connection error. Please try again.",
		      "reason" : "backendError"
		    } ],
		    "state" : "DONE"
		  }
		}
     * @throws IOException 
     */
    protected Job retryFailedBigQueryJob(Job job) throws IOException {
        String jobId = job.getJobReference().getJobId();
        log.debug("Retrying failed job: " + jobId);
        log.debug("Error result" + job.getStatus().getErrorResult());
        JobConfiguration config = job.getConfiguration();
        //String newCompletedJobId = null;
        if((config != null) && (config.getQuery() != null)) {
            // get the query
            String query = config.getQuery().getQuery();
            // re-execute the query
            ApiUtils.block(this.getApiRecheckDelay());
            
            BigDataTable table = null;
            
            if(BooleanUtils.isTrue(config.getQuery().getAllowLargeResults()) && 
                    (config.getQuery().getDestinationTable() != null)) {
                
                TableReference tableRef = config.getQuery().getDestinationTable();
                table = new BigDataTable(tableRef.getDatasetId() + '.' + StringUtils.split(tableRef.getTableId(), '_')[0]); 
            }
            
            String newJobId = startBigDataQuery(query, table);
            ApiUtils.block(this.getApiRecheckDelay());
            job = waitForBigQueryJobResults(newJobId, false, false);
        }
        return job;
    }

    /**
     * Print the stats of a big query job
     * @param job
     */
    protected void printJobStats(Job job) {
        try {
            JobStatistics stats = job.getStatistics();
            JobConfiguration config = job.getConfiguration();
            // log the query
            if(config != null) {
                log.debug("query: " + (config.getQuery() != null ? config.getQuery().getQuery() : ""));
            }
            // log the total bytes processed
            JobStatistics2 qStats = stats.getQuery();
            if(qStats != null) {
                log.debug("Total Bytes processed: " + ((double) qStats.getTotalBytesProcessed() / FileUtils.ONE_GB) + " GB");
                log.debug("Cache hit: " + qStats.getCacheHit());
            }

            JobStatistics3 lStats = stats.getLoad();
            if(lStats != null) {
                log.debug("Output rows: " + lStats.getOutputRows());

            }

            long time = stats.getEndTime() - stats.getCreationTime();
            log.debug("Elapsed query time (ms): " + time);
            log.debug("Elapsed query time (s): " + TimeUnit.MILLISECONDS.toSeconds(time));

        } catch(Exception e) {
            log.warn("failed to log job stats", e);
        }
    }
    
    /**
     * Find the records written from the job statistics
     * "queryPlan" : [ {
        "computeRatioAvg" : 0.8558157510920105,
        "computeRatioMax" : 1.0,
        "id" : "1",
        "name" : "Stage 1",
        "readRatioAvg" : 0.06898310223119479,
        "readRatioMax" : 0.08398906138792274,
        "recordsRead" : "14936600",
        "recordsWritten" : "8091263",
        "steps" : [ {
          "kind" : "READ",
          "substeps" : [ "object, predicate, subject", "FROM ontologies.swetodblp1", "WHERE LOGICAL_OR(LOGICAL_AND(EQUAL(predicate, 0), ...), ...)" ]
        }, {
          "kind" : "WRITE",
          "substeps" : [ "object, predicate, subject", "TO __output" ]
        } ],
        "waitRatioAvg" : 0.013799600438590943,
        "waitRatioMax" : 0.013799600438590943,
        "writeRatioAvg" : 0.3758086421588464,
        "writeRatioMax" : 0.49154118427586363
      } ],
     * 
     * @param job
     * @return
     */
    protected Long getBigQueryResultRows(Job job) {
        Long rows = null;
        if(job.getStatistics() != null && 
                job.getStatistics().getQuery() != null &&
                job.getStatistics().getQuery().getQueryPlan() != null) {
            
            List<ExplainQueryStage> explains = job.getStatistics().getQuery().getQueryPlan();
            
            if(explains.size() > 0) {
                ExplainQueryStage stage = explains.get(0);
                rows = stage.getRecordsWritten();
            }
            
        }
        
        return rows;
    }

    /**
     * Get a page of Bigquery rows
     * CONFIG: {
		 "kind": "bigquery#job",
		 "etag": "\"QPJfVWBscaHhAhSLq0k5xRS6X5c/eSppPGGASS7YbZBbC4v1q6lTcGM\"",
		 "id": "ecarf-1000:job_CaN3ROCFJdK30hBl7GBMmnvspgc",
		 "selfLink": "https://www.googleapis.com/bigquery/v2/projects/ecarf-1000/jobs/job_CaN3ROCFJdK30hBl7GBMmnvspgc",
		 "jobReference": {
		  "projectId": "ecarf-1000",
		  "jobId": "job_CaN3ROCFJdK30hBl7GBMmnvspgc"
		 },
		 "configuration": {
		  "query": {
		   "query": "select subject from swetodblp.swetodblp_triple where object = \"\u003chttp://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings1\u003e\";",
		   "destinationTable": {
		    "projectId": "ecarf-1000",
		    "datasetId": "_f14a24df5a43859914cb508177aa01d64466d055",
		    "tableId": "anonc6f8ec7bfe8bbd6bd76bbac6ad8db54482cd8209"
		   },
		   "createDisposition": "CREATE_IF_NEEDED",
		   "writeDisposition": "WRITE_TRUNCATE"
		  }
		 },
		 "status": {
		  "state": "DONE"
		 },
		 "statistics": {
		  "creationTime": "1398030237040",
		  "startTime": "1398030237657",
		  "endTime": "1398030237801",
		  "totalBytesProcessed": "0",
		  "query": {
		   "totalBytesProcessed": "0",
		   "cacheHit": true
		  }
		 }
		}
     * @return
     * @throws IOException 
     */
    protected GetQueryResultsResponse getQueryResults(String jobId, String pageToken) throws IOException {
        int retries = 3;
        boolean retrying = false;
        GetQueryResultsResponse queryResults = null;
        do {
            try {
                queryResults = this.getBigquery().jobs()
                        .getQueryResults(projectId, jobId)
                        .setPageToken(pageToken)
                        .setOauthToken(this.getOAuthToken())
                        .execute();

                retrying = false;

            } catch(IOException e) {
                log.error("failed to query job", e);
                retries--;
                if(retries == 0) {
                    throw e;
                }
                int delay = this.getApiRecheckDelay();
                log.debug("Retrying again in " + delay + " seconds");
                ApiUtils.block(delay);
                retrying = true;
            }

        } while(retrying);

        return queryResults;
    }
    
    /**
     * A job that extracts data from a table.
     * @param bigquery Bigquery service to use
     * @param cloudStoragePath Cloud storage bucket we are inserting into
     * @param table Table to extract from
     * @return The job to extract data from the table
     * @throws IOException Thrown if error connceting to Bigtable
     */
    // [START extract_job]
    protected Job runBigQueryExtractJob(final List<String> cloudStorageUris, final TableReference table) throws IOException {
        
        log.debug("Saving table: " + table + ", to cloud storage files: " + cloudStorageUris);
        
        //https://cloud.google.com/bigquery/exporting-data-from-bigquery
        JobConfigurationExtract extract = new JobConfigurationExtract()
            .setSourceTable(table)
            .setDestinationFormat("CSV")
            .setCompression("GZIP")
            .setDestinationUris(cloudStorageUris);

        return this.getBigquery().jobs().insert(table.getProjectId(),
                new Job().setConfiguration(new JobConfiguration().setExtract(extract)))
                .setOauthToken(this.getOAuthToken())
                .execute();
    }
    


    @Override
    public QueryStats saveBigQueryResultsToFile(String jobId, String filename, String bucket, Integer minFiles,
            int directDownloadRowLimit) throws IOException {
        
        Job queryJob = this.waitForBigQueryJobResults(jobId, false, true);
        Long rows = this.getBigQueryResultRows(queryJob);
        QueryStats stats = null;
        
        if(rows == null) {
            // cached query?
            rows = 0L;
        }
                
        log.debug("Downloading " + rows + " rows from BigQuery for jobId: " + jobId);
        
        if(rows > directDownloadRowLimit) {
            
            stats = this.saveBigQueryResultsToCloudStorage(jobId, queryJob, bucket, filename, minFiles);
            
        } else {
            
            stats = this.saveBigQueryResultsToFile(jobId, queryJob, FileUtils.TEMP_FOLDER + filename);
        }
        
        return stats;
    }
    
    @Override
    public QueryStats saveBigQueryResultsToCloudStorage(String jobId, String bucket, String filename) throws IOException {
        return this.saveBigQueryResultsToCloudStorage(jobId, null, bucket, filename, null);
    }
    
    /**
     * 
     * @param jobId
     * @param queryJob
     * @param bucket
     * @param filename
     * @param minFiles
     * @return
     * @throws IOException
     */
    protected QueryStats saveBigQueryResultsToCloudStorage(String jobId, Job queryJob, String bucket, String filename, Integer minFiles) throws IOException {
        
        // wait for the query job to complete
        if(queryJob == null) {
            queryJob = this.waitForBigQueryJobResults(jobId, true, false);
        }
                
        Stopwatch stopwatch = Stopwatch.createStarted();
        
        QueryStats stats = new QueryStats();
        
        // get the temporary table details
        TableReference table = queryJob.getConfiguration().getQuery().getDestinationTable();
        
        if(queryJob.getStatistics() != null) {
            stats.setTotalProcessedBytes(queryJob.getStatistics().getTotalBytesProcessed());
            Long rows = this.getBigQueryResultRows(queryJob);
            if(rows != null) {
                stats.setTotalRows(BigInteger.valueOf(rows));
            }
        }
        
        List<String> cloudStorageUris = new ArrayList<>();
        
        if((minFiles == null) || (minFiles <= 1)) {
            // use single wildcard URI
            // cloudex-processor-1456752400618_1456752468023_QueryResults_0_*
            cloudStorageUris.add(CLOUD_STORAGE_PREFIX + bucket + "/" + filename + WILDCARD_SUFFIX);
            
        }  else {
            // Multiple wildcard URIs
            // cloudex-processor-1456752400618_1456752468023_QueryResults_0_1_*
            // cloudex-processor-1456752400618_1456752468023_QueryResults_0_2_*
            String baseUri = CLOUD_STORAGE_PREFIX + bucket + "/" + filename + "_";
            
            for(int i = 1; i <= minFiles; i++) {
                
                cloudStorageUris.add(baseUri + i + WILDCARD_SUFFIX);
            }
        }
        
        Job extractJob = runBigQueryExtractJob(cloudStorageUris, table);
        
        extractJob = this.waitForBigQueryJobResults(extractJob.getJobReference().getJobId(), false, true);

        // list the relevant export files in cloud storage i.e.
        // cloudex-processor-1456752400618_1456752468023_QueryResults_0_000000000000   
        // cloudex-processor-1456752400618_1456752468023_QueryResults_0_000000000001    
        // cloudex-processor-1456752400618_1456752468023_QueryResults_0_000000000002
        List<io.cloudex.framework.cloud.entities.StorageObject> objects = this.listCloudStorageObjects(bucket);
        
        List<String> files = new ArrayList<>();
        
        for(io.cloudex.framework.cloud.entities.StorageObject object: objects) {
            String name = object.getName();
            if(name.startsWith(filename)) {
                files.add(name);
            }
        }
        
        for(String file: files) {

            log.debug("Downloading query results file: " + file);
            
            String localFile = FileUtils.TEMP_FOLDER + file;
            
            this.downloadObjectFromCloudStorage(file, localFile, bucket);
            
            stats.getOutputFiles().add(localFile);
        }
        
        log.debug("BigQuery query data saved successfully, timer: " + stopwatch);
        
        return stats;
    }

    @Override
    public QueryStats saveBigQueryResultsToFile(String jobId, String filename) throws IOException {
        return this.saveBigQueryResultsToFile(jobId, null, filename);
    }
    
    /**
     * Polls a big data job and once done save the results to a file
     * @param jobId
     * @param filename
     * @throws IOException
     * FIXME rename to saveBigDataResultsToFile
     */
    public QueryStats saveBigQueryResultsToFile(String jobId, Job queryJob, String filename) throws IOException {
        // query with retry support
        String completedJob;
        
        if(queryJob == null) {
            completedJob = checkBigQueryJobResults(jobId, true, false);
        } else {
            completedJob = queryJob.getJobReference().getJobId();
        }
        
        Joiner joiner = Joiner.on(',');
        String pageToken = null;
        BigInteger totalRows = null;
        Long totalBytes = null;
        Integer numFields = null;
        
        Stopwatch stopwatch = Stopwatch.createStarted();
        
        try(PrintWriter writer = new PrintWriter(new FileOutputStream(filename))) {

            do {

                GetQueryResultsResponse queryResult = this.getQueryResults(completedJob, pageToken);

                pageToken = queryResult.getPageToken();
                log.debug("Page token: " + pageToken);

                if(totalRows == null) {
                    totalRows = queryResult.getTotalRows();
                    numFields = queryResult.getSchema().getFields().size();
                    totalBytes = queryResult.getTotalBytesProcessed();
                    log.debug("Total rows for query: " + totalRows);
                }

                List<TableRow> rows = queryResult.getRows();

                if(rows != null) {
                    log.debug("Saving " + rows.size() + ", records to file: " + filename);

                    // save as CSV and properly escape the data to avoid failures on parsing
                    // one field only
                    if(numFields == 1) {
                        for (TableRow row : rows) {
                            writer.println(StringEscapeUtils.escapeCsv((String) row.getF().get(0).getV()));		
                        }

                    } else {
                        // multiple fields
                        for (TableRow row : rows) {
                            
                            List<Object> fields = new ArrayList<>();
                            
                            for (TableCell field : row.getF()) {
                                
                                if(Data.isNull(field.getV())) {
                                    
                                    fields.add("");
                                    
                                } else {
                                    
                                    fields.add(StringEscapeUtils.escapeCsv((String) field.getV()));
                                }
                            }
                            writer.println(joiner.join(fields));		
                        }
                    }
                }

            } while((pageToken != null) && !BigInteger.ZERO.equals(totalRows));
        }
        
        log.debug("BigQuery query data saved successfully, timer: " + stopwatch);
        
        QueryStats stats = new QueryStats(totalRows, totalBytes);
        stats.getOutputFiles().add(filename);
        return stats;
    }



    /* (non-Javadoc)
     * @see io.cloudex.framework.cloud.api.CloudService#getMaximumMetaDataSize()
     */
    @Override
    public int getMaximumMetaDataSize() {
        return maximumMetaDataSize;
    }


    /* (non-Javadoc)
     * @see io.cloudex.framework.cloud.api.CloudService#getApiRecheckDelay()
     */
    @Override
    public int getApiRecheckDelay() {
        return apiRecheckDelay;
    }


    /**
     * @param maximumMetaDataSize the maximumMetaDataSize to set
     */
    @Override
    public void setMaximumMetaDataSize(int maximumMetaDataSize) {
        this.maximumMetaDataSize = maximumMetaDataSize;
    }


    /**
     * @param apiRecheckDelay the apiRecheckDelay to set
     */
    @Override
    public void setApiRecheckDelay(int apiRecheckDelay) {
        this.apiRecheckDelay = apiRecheckDelay;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).
                append("projectId", this.projectId).
                append("instanceId", this.instanceId).
                append("zone", this.zone).
                toString();
    }


    /**
     * @param projectId the projectId to set
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    /**
     * @param zone the zone to set
     */
    public void setZone(String zone) {
        this.zone = zone;
    }


    /**
     * @param serviceAccount the serviceAccount to set
     */
    public void setServiceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
    }


    /**
     * @param scopes the scopes to set
     */
    public void setScopes(List<String> scopes) {
        this.scopes = scopes;
    }


    /**
     * @param instanceId the instanceId to set
     */
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }


    /**
     * @return the instanceId
     */
    @Override
    public String getInstanceId() {
        return instanceId;
    }


    /**
     * @return the projectId
     */
    public String getProjectId() {
        return projectId;
    }


    /**
     * @return the zone
     */
    public String getZone() {
        return zone;
    }


    /**
     * @param maxBigQueryRequestSize the maxBigQueryRequestSize to set
     */
    public void setMaxBigQueryRequestSize(int maxBigQueryRequestSize) {
        this.maxBigQueryRequestSize = maxBigQueryRequestSize;
    }


    /**
     * @param bigQueryStreamDelay the bigQueryStreamDelay to set
     */
    public void setBigQueryStreamDelay(int bigQueryStreamDelay) {
        this.bigQueryStreamDelay = bigQueryStreamDelay;
    }


    /**
     * @param bigQueryStreamFailRetries the bigQueryStreamFailRetries to set
     */
    public void setBigQueryStreamFailRetries(int bigQueryStreamFailRetries) {
        this.bigQueryStreamFailRetries = bigQueryStreamFailRetries;
    }


    @Override
    public void setRemote(boolean remote) {
        this.remote = remote;
    }


    @SuppressWarnings("rawtypes")
    @Override
    public void setAuthenticationProvider(AuthenticationProvider provider) {
        this.authenticationProvider = provider;
        
    }

}
