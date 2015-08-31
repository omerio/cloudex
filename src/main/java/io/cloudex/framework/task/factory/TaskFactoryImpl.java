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

import io.cloudex.framework.cloud.CloudService;
import io.cloudex.framework.cloud.VmMetaData;
import io.cloudex.framework.components.Context;
import io.cloudex.framework.config.TaskConfig;
import io.cloudex.framework.exceptions.ClassInstantiationException;
import io.cloudex.framework.exceptions.InstancePopulationException;
import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.task.Task;
import io.cloudex.framework.types.TargetType;
import io.cloudex.framework.utils.ObjectUtils;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.Validate;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TaskFactoryImpl implements TaskFactory {

    @Override
    public Task getTask(TaskConfig config, Context context, CloudService cloudService) 
            throws ClassInstantiationException, InstancePopulationException, IOException {
        
        Validate.notNull(config);
        Validate.notNull(config.getTarget());
  
        // TODO Deal with remote code
        Task task = null;
        if(TargetType.COORDINATOR.equals(config.getTarget())) {
            
            String className = config.getClassName();
            Validate.notNull(className, "task className is required");
            
            task = ObjectUtils.createInstance(CommonTask.class, config.getClassName());;
            
            Map<String, Object> input = context.resolveValues(config.getInput());
            ObjectUtils.populate(task, input);
            task.setCloudService(cloudService);
            
        }
        
        return task;
        
    }
    
    @Override
    public Task getTask(VmMetaData metaData, CloudService cloudService) throws ClassInstantiationException, 
        InstancePopulationException, IOException {
        
        Validate.notNull(metaData, "metaData is required");
        
        Task task = null;
        // use the metadata to auto populate the class properties, we strip a user- prefix on the metadata names then
        // just use the result as the property name
        String className = metaData.getTaskClass();
        
        Validate.notNull(className, "task className is required");
        // TODO deal with remote code
        
        task = ObjectUtils.createInstance(CommonTask.class, className);
        Map<String, String> input = metaData.getUserMetaData();
        ObjectUtils.populate(task, input);
        task.setCloudService(cloudService);
        
        return task;
    }

}
