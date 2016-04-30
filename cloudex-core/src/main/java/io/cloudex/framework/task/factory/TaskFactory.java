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


package io.cloudex.framework.task.factory;

import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.cloud.entities.VmMetaData;
import io.cloudex.framework.components.Context;
import io.cloudex.framework.config.TaskConfig;
import io.cloudex.framework.exceptions.ClassInstantiationException;
import io.cloudex.framework.exceptions.InstancePopulationException;
import io.cloudex.framework.task.Task;

import java.io.IOException;

/**
 * A factory for creating cloudex tasks
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface TaskFactory {

    /**
     * Return a task from the provided task config, this is used by the Coordinator
     * @param config - the task configuration
     * @param context - the coordinator job context
     * @param cloudService - the cloud service for the task to use
     * @return task implementation
     * @throws ClassInstantiationException if the instantiation of the task fails
     * @throws InstancePopulationException if the population of the task fails
     * @throws IOException if any of the cloud api calls fail
     */
    public Task getTask(TaskConfig config, Context context, CloudService cloudService) 
            throws ClassInstantiationException, InstancePopulationException, IOException;
    
    
    /**
     * Return a task from the provided metaData, this is used by the Processor
     * @param metaData - the current processor metadata
     * @param cloudService - the cloud service for the task to use
     * @return task implementation
     * @throws ClassInstantiationException if the instantiation of the task fails
     * @throws InstancePopulationException if the population of the task fails
     * @throws IOException if any of the cloud api calls fail
     */
    public Task getTask(VmMetaData metaData, CloudService cloudService) throws ClassInstantiationException, 
        InstancePopulationException, IOException;
    
}
