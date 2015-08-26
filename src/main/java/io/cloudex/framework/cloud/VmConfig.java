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

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class VmConfig {

    private String instanceId;

    // for Google, this is an example
    // https://www.googleapis.com/compute/v1/projects/ecarf-1000/zones/us-central1-a
    private String zoneId;

    // for Google, this is an example
    // https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20140318
    private String imageId;

    // https://www.googleapis.com/compute/v1/projects/ecarf-1000/zones/us-central1-a/machineTypes/f1-micro
    private String vmType;

    // for Google, this is an example
    // https://www.googleapis.com/compute/v1/projects/ecarf-1000/global/networks/default
    private String networkId;

    // for Google, this is an example
    // https://www.googleapis.com/compute/v1/projects/<project-id>/zones/<zone>/diskTypes/pd-ssd
    // https://www.googleapis.com/compute/v1/projects/<project-id>/zones/<zone>/diskTypes/pd-standard
    private String diskType;

    private String startupScript;

    private VmMetaData metaData;

    /**
     * @return the instanceId
     */
    public String getInstanceId() {
        return instanceId;
    }

    /**
     * @param instanceId the instanceId to set
     */
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    /**
     * @return the zoneId
     */
    public String getZoneId() {
        return zoneId;
    }

    /**
     * @param zoneId the zoneId to set
     */
    public void setZoneId(String zoneId) {
        this.zoneId = zoneId;
    }

    /**
     * @return the imageId
     */
    public String getImageId() {
        return imageId;
    }

    /**
     * @param imageId the imageId to set
     */
    public void setImageId(String imageId) {
        this.imageId = imageId;
    }

    /**
     * @return the metaData
     */
    public VmMetaData getMetaData() {
        return metaData;
    }

    /**
     * @param metaData the metaData to set
     */
    public void setMetaData(VmMetaData metaData) {
        this.metaData = metaData;
    }


    /**
     * @return the vmType
     */
    public String getVmType() {
        return vmType;
    }

    /**
     * @param vmType the vmType to set
     */
    public void setVmType(String vmType) {
        this.vmType = vmType;
    }

    /**
     * @return the networkId
     */
    public String getNetworkId() {
        return networkId;
    }

    /**
     * @param networkId the networkId to set
     */
    public void setNetworkId(String networkId) {
        this.networkId = networkId;
    }

    /**
     * @return the startupScript
     */
    public String getStartupScript() {
        return startupScript;
    }

    /**
     * @param startupScript the startupScript to set
     */
    public void setStartupScript(String startupScript) {
        this.startupScript = startupScript;
    }

    /**
     * @return the diskType
     */
    public String getDiskType() {
        return diskType;
    }

    /**
     * @param diskType the diskType to set
     */
    public void setDiskType(String diskType) {
        this.diskType = diskType;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }



}
