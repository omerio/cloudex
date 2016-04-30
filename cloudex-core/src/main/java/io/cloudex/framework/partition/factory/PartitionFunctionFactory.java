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


package io.cloudex.framework.partition.factory;

import io.cloudex.framework.components.Context;
import io.cloudex.framework.config.PartitionConfig;
import io.cloudex.framework.exceptions.ClassInstantiationException;
import io.cloudex.framework.exceptions.InstancePopulationException;
import io.cloudex.framework.partition.PartitionFunction;

/**
 * A factory responsible for creating partitioning functions of type PartitionFunction
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface PartitionFunctionFactory {
    
    /**
     * Return a partitioning function based on config provided
     * @param config - the partitioning config
     * @param context - the context to resolve the values
     * @return a partitioning function for the config provided
     * @throws ClassInstantiationException if the instantiation of the partition function class fails
     * @throws InstancePopulationException if the population of the partition function instance fails
     */
    public PartitionFunction getPartitionFunction(PartitionConfig config, Context context) 
            throws ClassInstantiationException, InstancePopulationException;

}
