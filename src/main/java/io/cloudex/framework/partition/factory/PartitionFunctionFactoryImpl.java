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

package io.cloudex.framework.partition.factory;

import io.cloudex.framework.components.Context;
import io.cloudex.framework.config.PartitionConfig;
import io.cloudex.framework.exceptions.ClassInstantiationException;
import io.cloudex.framework.exceptions.InstancePopulationException;
import io.cloudex.framework.partition.PartitionFunction;
import io.cloudex.framework.partition.builtin.BuiltInPartitionFunctions;
import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.types.PartitionType;
import io.cloudex.framework.utils.ObjectUtils;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * A factory responsible for creating partitioning functions of type PartitionFunction
 * @author Omer Dawelbeit (omerio)
 *
 */
public class PartitionFunctionFactoryImpl implements PartitionFunctionFactory {


    /**
     * Return a partitioning function based on config provided
     * @param config - the partitioning config
     * @param context - the context to resolve the values
     * @return a partitioning function for the config provided
     * @throws ClassInstantiationException if the instantiation of the partition function class fails
     * @throws InstancePopulationException if the population of the partition function instance fails
     */
    @SuppressWarnings("unchecked")
    public PartitionFunction getPartitionFunction(PartitionConfig config, Context context) 
            throws ClassInstantiationException, InstancePopulationException {

        Validate.notNull(config, "config is required");

        PartitionFunction function = null;

        if(PartitionType.FUNCTION.equals(config.getType())) {

            Validate.notNull(config.getInput(), "config input is required");
            Validate.notNull(context, "context is required");

            Map<String, Object> input = context.resolveValues(config.getInput());

            List<? extends Item> items = (List<? extends Item>) input.get(PartitionFunction.ITEMS_KEY);

            Validate.notNull(items, "items to partition are required");

            if(StringUtils.isNotBlank(config.getFunctionName())) {

                function = BuiltInPartitionFunctions.getFunction(config.getFunctionName());

            } else if(StringUtils.isNotBlank(config.getClassName())) {

                function = ObjectUtils.createInstance(PartitionFunction.class, config.getClassName());

            } else {
                throw new IllegalArgumentException("either functionName or className should be provided");
            }

            ObjectUtils.populate(function, input);
            function.setItems(items);

        }

        return function;
    }

}
