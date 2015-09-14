/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2015, cloudex.io
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


package io.cloudex.framework.cloud.entities;

import io.cloudex.framework.config.VmConfig;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Represent a VM instance that has been started and has incurred cost. The
 * VM might currently be running running or stopped
 *  
 * @author Omer Dawelbeit (omerio)
 *
 */
public class VmInstance implements Serializable {

    private static final long serialVersionUID = -4066407426066459562L;
    
    private static final double SECONDS_IN_HOUR = 3600.0;
    
    // config of the VM
    private VmConfig vmConfig;
    
    // Start date time
    private Date start;
    
    // end date time
    private Date end;
    
    
    public VmInstance() {
        super();
    }

    /**
     * @param vmConfig - the vmConfig of this instance
     * @param start - the date this vm was started
     */
    public VmInstance(VmConfig vmConfig, Date start) {
        super();
        this.vmConfig = vmConfig;
        this.start = start;
    }
    
    public boolean isRunning() {
        return (this.start != null) && (this.end == null);
    }
    
    /**
     * Get the approximate usage cost of this VM
     * @return the usage cost so far of this VM instance
     */
    public double getCost() {
        double cost = 0.0;
        
        if(this.start != null) {
            
            Validate.notNull(this.vmConfig);
            
            Date endDate = this.end;
            
            if(endDate == null) {
                endDate = new Date();
            }
            
            double elapsed = endDate.getTime() - this.start.getTime();
            Long minUsage = this.vmConfig.getMinUsage();
            if((minUsage != null) && (minUsage > elapsed)) {
                elapsed = minUsage;
            
            } else {
                elapsed = elapsed / 1000;
                
                // elapsed is in seconds, round up to the nearest minute
                double mins = Math.ceil(elapsed / 60);
                elapsed = (long) (mins * 60);
            }
            
            Double hourlyCost = this.vmConfig.getCost();
            
            if(hourlyCost != null) {
                cost = hourlyCost * elapsed / SECONDS_IN_HOUR;
            }
            
        }
        
        return cost;
    }

    /**
     * @return the vmConfig
     */
    public VmConfig getVmConfig() {
        return vmConfig;
    }

    /**
     * @param vmConfig the vmConfig to set
     */
    public void setVmConfig(VmConfig vmConfig) {
        this.vmConfig = vmConfig;
    }

    /**
     * @return the start
     */
    public Date getStart() {
        return start;
    }

    /**
     * @param start the start to set
     */
    public void setStart(Date start) {
        this.start = start;
    }

    /**
     * @return the end
     */
    public Date getEnd() {
        return end;
    }

    /**
     * @param end the end to set
     */
    public void setEnd(Date end) {
        this.end = end;
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("start", start)
            .append("end", end)
            .append("vmConfig", vmConfig)
            .append("cost", this.getCost())
            .toString();
    }


}
