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
import org.apache.commons.lang3.builder.EqualsBuilder;
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
    
    // disk size in GB
    private Long diskSize;

    private String startupScript;

    private transient VmMetaData metaData;
    
    private transient String instanceId;
    
    // VM specification settings
    // The hourly cost of this VM
    private Double cost;
    
    // the minimum billed usage in seconds, some cloud providers like Google 
    // for example bill for a minimum of 10mins
    private Long minUsage;
 
    // The VM system memory in GB
    private Integer memory;
    
    // The number of Virtual CPUs
    private Integer cores;
    
    // Should this VM be reused, default is true
    private Boolean reuse;
    
    // Set to true to not give the VM an external IP
    private Boolean noExternalIp;

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

    /**
     * @return the cost
     */
    public Double getCost() {
        return cost;
    }

    /**
     * Set the hourly cost of this VM
     * @param cost the cost to set
     */
    public void setCost(Double cost) {
        this.cost = cost;
    }

    /**
     * @return the minUsage
     */
    public Long getMinUsage() {
        return minUsage;
    }

    /**
     * Set the minimum billed usage in seconds, some cloud providers like Google 
    // for example bill for a minimum of 10mins
     * @param minUsage the minUsage to set
     */
    public void setMinUsage(Long minUsage) {
        this.minUsage = minUsage;
    }

    /**
     * @return the memory
     */
    public Integer getMemory() {
        return memory;
    }

    /**
     * set the VM system memory in GB
     * @param memory the memory to set
     */
    public void setMemory(Integer memory) {
        this.memory = memory;
    }

    /**
     * @return the cores
     */
    public Integer getCores() {
        return cores;
    }

    /**
     * Set the number of Virtual CPUs
     * @param cores the cores to set
     */
    public void setCores(Integer cores) {
        this.cores = cores;
    }

    /**
     * @return the reuse
     */
    public Boolean getReuse() {
        return reuse;
    }

    /**
     * Set if this VM should be reused, default is true
     * @param reuse the reuse to set
     */
    public void setReuse(Boolean reuse) {
        this.reuse = reuse;
    }

    /**
     * @return the diskSize
     */
    public Long getDiskSize() {
        return diskSize;
    }

    /**
     * @param diskSize the diskSize to set
     */
    public void setDiskSize(Long diskSize) {
        this.diskSize = diskSize;
    }

    /**
     * @return the noExternalIp
     */
    public Boolean getNoExternalIp() {
        return noExternalIp;
    }

    /**
     * @param noExternalIp the noExternalIp to set
     */
    public void setNoExternalIp(Boolean noExternalIp) {
        this.noExternalIp = noExternalIp;
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
        vmConfig.setDiskSize(this.diskSize);
        vmConfig.setImageId(this.imageId);
        vmConfig.setNetworkId(this.networkId);
        vmConfig.setStartupScript(this.startupScript);
        vmConfig.setVmType(this.vmType);
        vmConfig.setZoneId(this.zoneId);
        vmConfig.setNoExternalIp(this.noExternalIp);
        
        // settings
        vmConfig.setCores(this.cores);
        vmConfig.setCost(this.cost);
        vmConfig.setMemory(this.memory);
        vmConfig.setReuse(this.reuse);
        vmConfig.setMinUsage(this.minUsage);
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
        
        if(config.getDiskSize() != null) {
            vmConfig.setDiskSize(this.diskSize);
        }
        
        if(config.getCores() != null) {
            vmConfig.setCores(this.cores);
        }

        if(config.getCost() != null) {
            vmConfig.setCost(this.cost);
        }

        if(config.getMemory() != null) {
            vmConfig.setMemory(this.memory);
        }

        if(config.getReuse() != null) {
            vmConfig.setReuse(this.reuse);
        }

        if(config.getMinUsage() != null) {
            vmConfig.setMinUsage(this.minUsage);
        }
        
        if(config.getNoExternalIp() != null) {
            vmConfig.setNoExternalIp(this.noExternalIp);
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
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) { 
            return false; 
        }
        if (obj == this) { 
            return true; 
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        VmConfig rhs = (VmConfig) obj;
        return new EqualsBuilder()
            //.appendSuper(super.equals(obj))
            .append(this.diskType, rhs.getDiskType())
            .append(this.diskSize, rhs.getDiskSize())
            .append(this.noExternalIp, rhs.getNoExternalIp())
            .append(this.imageId, rhs.getImageId())
            .append(this.networkId, rhs.getNetworkId())
            .append(this.startupScript, rhs.getStartupScript())
            .append(this.vmType, rhs.getVmType())
            .append(this.zoneId, rhs.getZoneId())
            .isEquals();
    }

}
