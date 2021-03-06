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


package io.cloudex.framework.partition.builtin;

import io.cloudex.framework.exceptions.ClassInstantiationException;
import io.cloudex.framework.partition.PartitionFunction;
import io.cloudex.framework.utils.ObjectUtils;

/**
 * An enum of all built-in workload partitioning functions
 * @author Omer Dawelbeit (omerio)
 *
 */
public enum BuiltInPartitionFunctions {
    
    BinPackingPartition("io.cloudex.framework.partition.builtin.BinPackingPartition"),
    BinPackingPartition1("io.cloudex.framework.partition.builtin.BinPackingPartition1");
    
    public final String className;
    
    /**
     * @param className
     */
    private BuiltInPartitionFunctions(String className) {
        this.className = className;
    }

    /**
     * Get a built-in partition function instance from a function name;
     * @param functionName
     * @return
     * @throws ClassInstantiationException 
     */
    public static PartitionFunction getFunction(String functionName) throws ClassInstantiationException {
        BuiltInPartitionFunctions builtInFunction = BuiltInPartitionFunctions.valueOf(functionName);
        return ObjectUtils.createInstance(PartitionFunction.class, builtInFunction.className);
    }

}
