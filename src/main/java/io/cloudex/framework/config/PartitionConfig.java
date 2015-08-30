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

import io.cloudex.framework.partition.PartitionFunction;
import io.cloudex.framework.types.PartitionType;
import io.cloudex.framework.utils.ObjectUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Workload partition configurations
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class PartitionConfig implements Serializable    {

    private static final long serialVersionUID = -8323425744252645077L;

    @NotNull
    private PartitionType type;

    private String className;
    
    // prebuilt functions
    private String functionName;

    @NotNull
    private Map<String, String> input;

    private String output;

    /**
     * @return the type
     */
    public PartitionType getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(PartitionType type) {
        this.type = type;
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
     * @return the input
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
     * @return the output
     */
    public String getOutput() {
        return output;
    }

    /**
     * @param output the output to set.
     */
    public void setOutput(String output) {
        this.output = output;
    }

    /**
     * @return the functionName
     */
    public String getFunctionName() {
        return functionName;
    }

    /**
     * @param functionName the functionName to set
     */
    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }
    
    /**
     * Determine if this instance is valid
     * @return true if valid, false otherwise
     */
    public boolean valid() {
        
        return ObjectUtils.isValid(PartitionConfig.class, this) 
                && BooleanUtils.xor(new boolean [] {StringUtils.isBlank(this.className), 
                        StringUtils.isBlank(this.functionName)})
                && (PartitionType.ITEMS.equals(this.type) 
                        || (PartitionType.FUNCTION.equals(this.type) && StringUtils.isNotBlank(this.output)))
                && (this.input.get(PartitionFunction.ITEMS_KEY) != null);
        
    }
    
    /**
     * Get any validation errors
     * @return a list of validation messages
     */
    public List<String> getValidationErrors() {

        List<String> messages = ObjectUtils.getValidationErrors(PartitionConfig.class, this);
        
        if(!BooleanUtils.xor(new boolean [] {StringUtils.isBlank(this.className), 
                StringUtils.isBlank(this.functionName)})) {

            messages.add("either functionName or className is required");
        }
        
        if(PartitionType.FUNCTION.equals(this.type) && StringUtils.isBlank(this.output)) {
            messages.add("output is required for Function type partition");
        }
        
        if(this.input.get(PartitionFunction.ITEMS_KEY) == null) {
            messages.add("expecting an input with key items");
        }
        
        return messages;
    }


}
