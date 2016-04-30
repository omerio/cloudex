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

import io.cloudex.framework.types.ErrorAction;
import io.cloudex.framework.types.TargetType;
import io.cloudex.framework.utils.ObjectUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Represents Task configurations detailing how to instantiate a Task object. Details include the 
 * Task inputs, outputs, code, target, etc...
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TaskConfig implements Serializable {

    private static final long serialVersionUID = 3302466101507343648L;

    private String id;
    
    private String description;
    
    @NotNull
    private String className;
    
    // predefined task name
    //private String taskName;
    
    private Map<String, String> input;
    
    private Set<String> output;
    
    @NotNull
    private TargetType target;
    
    @NotNull
    private ErrorAction errorAction;
    
    private CodeConfig code;
    
    private PartitionConfig partitioning;
    
    /**
     * Provide the ability to run individual tasks on 
     * different VM configurations
     */
    private VmConfig vmConfig;
    
    /**
     * Provide the ability to lookup the vmConfig for this task from the 
     * job context, enables for dynamically configured VMS (Elasticity)
     */
    private String vmConfigReference;

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the className
     */
    public String getClassName() {
        return className;
    }

    /**
     * @param className the className to set
     */
    public void setClassName(String className) {
        this.className = className;
    }


    /**
     * @return input
     */
    public Map<String, String> getInput() {
        return input;
    }

    /**
     * @param input the input to set
     */
    public void setInput(Map<String, String> input) {
        this.input = input;
    }

    /**
     * @return output
     */
    public Set<String> getOutput() {
        return output;
    }

    /**
     * @param output the output to set
     */
    public void setOutput(Set<String> output) {
        this.output = output;
    }

    /**
     * @return target
     */
    public TargetType getTarget() {
        return target;
    }

    /**
     * @param target the target to set
     */
    public void setTarget(TargetType target) {
        this.target = target;
    }

    /**
     * @return errorAction
     */
    public ErrorAction getErrorAction() {
        return errorAction;
    }

    /**
     * @param errorAction the errorAction to set
     */
    public void setErrorAction(ErrorAction errorAction) {
        this.errorAction = errorAction;
    }

    /**
     * @return the code
     */
    public CodeConfig getCode() {
        return code;
    }

    /**
     * @param code the code to set
     */
    public void setCode(CodeConfig code) {
        this.code = code;
    }

    /**
     * @return the partitioning
     */
    public PartitionConfig getPartitioning() {
        return partitioning;
    }

    /**
     * @param partitioning the partitioning to set
     */
    public void setPartitioning(PartitionConfig partitioning) {
        this.partitioning = partitioning;
    }
    
    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
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
     * @return the vmConfigReference
     */
    public String getVmConfigReference() {
        return vmConfigReference;
    }

    /**
     * @param vmConfigReference the vmConfigReference to set
     */
    public void setVmConfigReference(String vmConfigReference) {
        this.vmConfigReference = vmConfigReference;
    }

    /**
     * check if this instance is valid
     * @return true if valid
     */
    public boolean valid() {
        return ObjectUtils.isValid(TaskConfig.class, this)  
                && ((TargetType.COORDINATOR.equals(this.target) && (this.vmConfig == null) 
                        && (this.vmConfigReference == null))
                        || (TargetType.PROCESSOR.equals(this.target) && (this.partitioning != null) 
                                && this.partitioning.valid() && (this.output == null)));
    }
    
    /**
     * Get any validation errors
     * @return a list of validation messages
     */
    public List<String> getValidationErrors() {

        List<String> messages = ObjectUtils.getValidationErrors(TaskConfig.class, this);
        
        if(TargetType.PROCESSOR.equals(this.target)) {

            if (this.partitioning == null) {
                messages.add("a valid partition config is required for processor tasks");
            }
            
            if(this.output != null) {
                messages.add("processor tasks should not have any output");
            }
            
        } else if(TargetType.COORDINATOR.equals(this.target) 
                && ((this.vmConfig != null) || (this.vmConfigReference != null))) {
            messages.add("vm config is not expected for coordinators");
        }

        if(this.partitioning != null) {
            messages.addAll(this.partitioning.getValidationErrors());
        }

        return messages;
    }

}
