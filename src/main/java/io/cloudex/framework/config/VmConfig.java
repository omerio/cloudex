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

package io.cloudex.framework.config;

import io.cloudex.framework.cloud.entities.VmMetaData;
import io.cloudex.framework.utils.ObjectUtils;

import java.io.Serializable;
import java.util.List;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class VmConfig implements Serializable {

    private static final long serialVersionUID = -6635094527551555857L;

    // for Google, this is an example
    // https://www.googleapis.com/compute/v1/projects/ecarf-1000/zones/us-central1-a
    @NotNull
    @Size(min = 1)
    private String zoneId;

    // for Google, this is an example
    // https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20140318
    @NotNull
    @Size(min = 1)
    private String imageId;

    // https://www.googleapis.com/compute/v1/projects/ecarf-1000/zones/us-central1-a/machineTypes/f1-micro
    @NotNull
    @Size(min = 1)
    private String vmType;

    // for Google, this is an example
    // https://www.googleapis.com/compute/v1/projects/ecarf-1000/global/networks/default
    @NotNull
    @Size(min = 1)
    private String networkId;

    // for Google, this is an example
    // https://www.googleapis.com/compute/v1/projects/<project-id>/zones/<zone>/diskTypes/pd-ssd
    // https://www.googleapis.com/compute/v1/projects/<project-id>/zones/<zone>/diskTypes/pd-standard
    @NotNull
    @Size(min = 1)
    private String diskType;

    private String startupScript;

    private transient VmMetaData metaData;
    
    private transient String instanceId;

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

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        ReflectionToStringBuilder builder = new ReflectionToStringBuilder(this);
        builder.setAppendTransients(true);
        return builder.toString();
    }
    
    /**
     * Make a selective copy of this instance
     * @return a copy of this vm config
     */
    public VmConfig copy() {
        VmConfig vmConfig = new VmConfig();
        vmConfig.setDiskType(this.diskType);
        vmConfig.setImageId(this.imageId);
        vmConfig.setNetworkId(this.networkId);
        vmConfig.setStartupScript(this.startupScript);
        vmConfig.setVmType(this.vmType);
        vmConfig.setZoneId(this.zoneId);
        return vmConfig;
    }
    
    /**
     * Returns a merge of a copy of this instance with some of the fields 
     * overwritten by the non null value in config 
     * @param config - the config to overwrite the returned copy
     * @return a merged copy of this instance with the provided config
     */
    public VmConfig merge(VmConfig config) {
        
        VmConfig vmConfig = this.copy();
        
        if(StringUtils.isNotBlank(config.getDiskType())) {
            vmConfig.setDiskType(config.getDiskType());
        }
        
        if(StringUtils.isNotBlank(config.getImageId())) {
            vmConfig.setImageId(config.getImageId());
        }
        
        if(StringUtils.isNotBlank(config.getNetworkId())) {
            vmConfig.setNetworkId(config.getNetworkId());
        }
        
        if(StringUtils.isNotBlank(config.getStartupScript())) {
            vmConfig.setStartupScript(config.getStartupScript());
        }
        
        if(StringUtils.isNotBlank(config.getVmType())) {
            vmConfig.setVmType(config.getVmType());
        }
        
        if(StringUtils.isNotBlank(config.getZoneId())) {
            vmConfig.setZoneId(config.getZoneId());
        }
        return vmConfig;
        
    }
    
    /**
     * check if this instance is valid
     * @return true if valid
     */
    public boolean valid() {
        return ObjectUtils.isValid(VmConfig.class, this);
    }
    
    /**
     * Get any validation errors
     * @return a list of validation messages
     */
    public List<String> getValidationErrors() {
        return ObjectUtils.getValidationErrors(VmConfig.class, this);
    }

}
