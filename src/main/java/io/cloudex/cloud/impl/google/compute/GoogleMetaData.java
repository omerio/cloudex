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
package io.cloudex.cloud.impl.google.compute;

import io.cloudex.framework.utils.ObjectUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class GoogleMetaData {
    
    private final static Log log = LogFactory.getLog(GoogleMetaData.class);

    // metadata server urls
    public static final String METADATA_SERVER_URL = "http://metadata.google.internal/computeMetadata/v1/";
    public static final String TOKEN_PATH = "instance/service-accounts/default/token";
    //private static final String SERVICE_ACCOUNT_PATH = "service-accounts/default/?recursive=true";
    public static final String INSTANCE_ALL_PATH = "instance/?recursive=true";
    //private static final String PROJECT_ALL_PATH = "project/?recursive=true";
    public static final String PROJECT_ID_PATH = "project/project-id";
    public static final String ATTRIBUTES_PATH = "instance/attributes/?recursive=true";
    // without timeout it doesn't return
    public static final String WAIT_FOR_CHANGE = "&wait_for_change=true&timeout_sec=360";

    // scopes
    public static final String DATASTORE_SCOPE = "https://www.googleapis.com/auth/datastore";

    public static final String RESOURCE_BASE_URL = "https://www.googleapis.com/compute/v1/projects/";
    public static final String NETWORK = "/global/networks/";
    public static final String ZONES = "/zones/";
    public static final String MACHINE_TYPES = "/machineTypes/";
    public static final String DISK_TYPES = "/diskTypes/";
    public static final String CENTO_IMAGE = "/centos-cloud/global/images/centos-6-v20140318";
    public static final String ACCESS_TOKEN = "access_token";
    public static final String EXPIRES_IN = "expires_in";
    public static final String PROJECT_ID = "projectId";
    public static final String ID = "id";
    public static final String HOSTNAME = "hostname";
    public static final String ZONE = "zone";
    public static final String ATTRIBUTES = "attributes";
    public static final String ITEMS = "items";
    public static final String FINGER_PRINT = "fingerprint";
    public static final String DONE = "DONE";
    public static final String EMAIL = "email";
    public static final String SCOPES = "scopes";
    public static final String SERVICE_ACCOUNTS = "serviceAccounts";
    public static final String DEFAULT = "default";
    public static final String IMAGE = "image";
    public static final String PERSISTENT = "PERSISTENT";
    public static final String MIGRATE = "MIGRATE";
    public static final String EXT_NAT = "External NAT";
    public static final String ONE_TO_ONE_NAT = "ONE_TO_ONE_NAT";
    public static final String STARTUP_SCRIPT = "startup-script";

    public static final String CLOUD_STORAGE_PREFIX = "gs://";
    public static final String NOT_FOUND = "404 Not Found";

    // BigQuery create/write disposition
    public static final String CREATE_NEVER = "CREATE_NEVER";
    public static final String CREATE_IF_NEEDED = "CREATE_IF_NEEDED";
    public static final String WRITE_APPEND = "WRITE_APPEND";

    public static final String TYPE_STRING = "STRING";
    public static final String TYPE_INTEGER = "INTEGER";
    
    // BigQuery column modes
    public static final String NULLABLE = "NULLABLE";
    public static final String REQUIRED = "REQUIRED";
    public static final String REPEATED = "REPEATED";
    
    public static final String TEMP_TABLE = "temp_table";

    // API error reasons
    public static final String RATE_LIMIT_EXCEEDED = "rateLimitExceeded";
    public static final String QUOTA_EXCEEDED = "quotaExceeded";
    
    
    /**
     * Call the metadata server, this returns details for the current instance not for
     * different instances. In order to retrieve the meta data of different instances
     * we just use the compute api, see getInstance
     * @param path
     * @return
     * @throws IOException
     */
    public static String getMetaData(String path) throws IOException {
        log.debug("Retrieving metadata from server, path: " + path);
        URL metadata = new URL(METADATA_SERVER_URL + path);
        HttpURLConnection con = (HttpURLConnection) metadata.openConnection();

        // optional default is GET
        //con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("Metadata-Flavor", "Google");

        int responseCode = con.getResponseCode();

        StringBuilder response = new StringBuilder();

        if(responseCode == 200) {
            try(BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
            }
        } else {
            String msg = "Metadata server responded with status code: " + responseCode;
            log.error(msg);
            throw new IOException(msg);
        }
        log.debug("Successfully retrieved metadata from server");

        return response.toString();
    }
    
    /**
     * Return the metadata as a map
     * @param path
     * @return
     */
    public static Map<String, Object> getMetaDataAsMap(String path) throws IOException {
        String metaData = getMetaData(path);
        
        return ObjectUtils.jsonToMap(metaData);
    }


}
