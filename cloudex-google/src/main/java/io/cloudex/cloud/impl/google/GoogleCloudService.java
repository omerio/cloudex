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

import io.cloudex.cloud.impl.google.bigquery.BigQueryStreamable;
import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.cloud.entities.BigDataTable;

import java.io.IOException;
import java.util.Collection;

/**
 * A Google Cloud Service specific API calls
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface GoogleCloudService extends CloudService {

    /**
     * Stream the provide collection of object into the provided BigQuery table
     * 
     * @param objects - the objects to stream, each must implement {@link BigQueryStreamable}
     * @param table - the BigQuery table to stream the data in {@link BigDataTable}
     * @throws IOException if any of the cloud api calls fail
     */
    public void streamObjectsIntoBigData(Collection<? extends BigQueryStreamable> objects, BigDataTable table) throws IOException;

    /**
     * Set the maximum meta data size in Bytes. Default is 32768
     * @param maximumMetaDataSize
     */
    public void setMaximumMetaDataSize(int maximumMetaDataSize);

    /**
     * Set the delay between api rechecks (polls). Default is 2 seconds
     * @param apiRecheckDelay
     */
    public void setApiRecheckDelay(int apiRecheckDelay);
    
    /**
     * The maximum number of rows of the BigQuery streaming insert. Default is
     * 100,000.
     * @param maxBigQueryRequestSize
     */
    public void setMaxBigQueryRequestSize(int maxBigQueryRequestSize);
    
    /**
     * The number of seconds to wait between the BigQuery streaming inserts to vaoid 
     * rateLimitExceeded errors. Default is 2 seconds
     * @param bigQueryStreamDelay
     */
    public void setBigQueryStreamDelay(int bigQueryStreamDelay);
    
    /**
     * The number of retries when the streamin of some rows into BigQuery fails, default is 
     * 3 retries
     * @param bigQueryStreamFailRetries
     */
    public void setBigQueryStreamFailRetries(int bigQueryStreamFailRetries);

}
