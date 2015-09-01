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

import java.io.IOException;

import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.cloud.entities.VmMetaData;

/**
 * Common cloud executable.
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public abstract class CommonExecutable implements Executable {

    private CloudService cloudService;

    private VmMetaData metaData;

    /**
     * @return cloudService.
     */
    public CloudService getCloudService() {
        return cloudService;
    }

    /**
     * @param cloudService the cloudService to set.
     * @throws IOException 
     */
    public void setCloudService(CloudService cloudService) throws IOException {
        this.cloudService = cloudService;
        
        // initialize the cloud service
        if(this.cloudService != null) {
            this.metaData = this.cloudService.init();
        }
    }


    /**
     * @return metaData.
     */
    public VmMetaData getMetaData() {
        return metaData;
    }


    /**
     * @param metaData the metaData to set.
     */
    public void setMetaData(VmMetaData metaData) {
        this.metaData = metaData;
    }

}
