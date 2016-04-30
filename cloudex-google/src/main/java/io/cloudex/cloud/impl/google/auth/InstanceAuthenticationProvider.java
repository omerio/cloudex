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


package io.cloudex.cloud.impl.google.auth;

import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.ACCESS_TOKEN;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.EXPIRES_IN;
import static io.cloudex.cloud.impl.google.compute.GoogleMetaData.TOKEN_PATH;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.cloudex.cloud.impl.google.compute.GoogleMetaData;
import io.cloudex.framework.cloud.api.AuthenticationProvider;
import io.cloudex.framework.utils.ObjectUtils;

/**
 * Provides authentication for code running locally on a Compute Engine instance. This will
 * use Compute Engine service account and the Meta Data server to retrieve OAuth tokens as
 * explained here {see https://cloud.google.com/compute/docs/authentication}. The authorize
 * method will return a OAuth token obtained from the meta data server
 * 
 *
 * @author Omer Dawelbeit (omerio)
 *
 */
public class InstanceAuthenticationProvider implements AuthenticationProvider<String> {

    private final static Log log = LogFactory.getLog(InstanceAuthenticationProvider.class);

    // service account access token retrieved from the metadata server
    private String accessToken;

    // the expiry time of the token
    private Date tokenExpire;


    /**
     * Returns an OAuth token, refreshes it if needed
     */
    @Override
    public String authorize() throws IOException {
        if((this.tokenExpire == null) || (new Date()).after(this.tokenExpire)) {
            this.refreshOAuthToken();
        }
        return this.accessToken;
    }


    /**
     * Retrieves a service account access token from the metadata server, the response has the format
     * {
          "access_token":"ya29.AHES6ZRN3-HlhAPya30GnW_bHSb_QtAS08i85nHq39HE3C2LTrCARA",
          "expires_in":3599,
          "token_type":"Bearer"
        }
     * @throws IOException
     * For more details see https://developers.google.com/compute/docs/authentication
     */
    protected void refreshOAuthToken() throws IOException {

        log.debug("Refreshing OAuth token from metadata server");
        Map<String, Object> token = ObjectUtils.jsonToMap(GoogleMetaData.getMetaData(TOKEN_PATH));
        this.accessToken = (String) token.get(ACCESS_TOKEN);

        Double expiresIn = (Double) token.get(EXPIRES_IN);
        this.tokenExpire = DateUtils.addSeconds(new Date(), expiresIn.intValue());

        log.debug("Successfully refreshed OAuth token from metadata server");

    }

}
