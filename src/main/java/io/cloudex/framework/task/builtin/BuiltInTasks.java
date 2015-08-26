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


package io.cloudex.framework.task.builtin;

import io.cloudex.framework.exceptions.ClassInstantiationException;
import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.utils.ObjectUtils;

/**
 * An enum of all built-in tasks
 * @author Omer Dawelbeit (omerio)
 *
 */
public enum BuiltInTasks {
    
    WaitForProcessorTask("io.cloudex.framework.task.builtin.WaitForProcessorTask");
    
    public final String className;

    /**
     * @param className
     */
    private BuiltInTasks(String className) {
        this.className = className;
    }
    
    /**
     * Get a built-in task instance from a task name
     * @param taskName
     * @return
     * @throws ClassInstantiationException
     */
    public static CommonTask getTask(String taskName) throws ClassInstantiationException {
        BuiltInTasks builtInTask = BuiltInTasks.valueOf(taskName);
        return ObjectUtils.createInstance(CommonTask.class, builtInTask.className);
    }
    

}
