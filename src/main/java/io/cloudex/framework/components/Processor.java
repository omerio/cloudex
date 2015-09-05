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

package io.cloudex.framework.components;

import io.cloudex.framework.CommonExecutable;
import io.cloudex.framework.cloud.api.ApiUtils;
import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.cloud.entities.VmMetaData;
import io.cloudex.framework.task.Task;
import io.cloudex.framework.task.factory.TaskFactory;
import io.cloudex.framework.task.factory.TaskFactoryImpl;
import io.cloudex.framework.types.ProcessorStatus;

import java.io.IOException;

import com.google.common.base.Stopwatch;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * The CloudEx Processor component. Use the Processor.Builder to create a new instance of this class
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Processor extends CommonExecutable {

    private static final Log log = LogFactory.getLog(Processor.class);

    private TaskFactory taskFactory;

    private boolean stop;

    /**
     * 
     * @param metaData - vm instance metadata 
     * @param cloudService - cloud service implementation
     * @throws IOException if cloud api calls fail
     */
    public Processor(CloudService cloudService) throws IOException {
        this(new Builder(cloudService));
    }

    /**
     * 
     * @param builder Coordinator builder
     * @throws IOException if cloud api calls fail 
     */
    Processor(Builder builder) throws IOException {
        super();
        
        Validate.notNull(builder.getCloudService(), "cloudService is required");
        
        this.taskFactory = builder.getTaskFactory();
        this.setCloudService(builder.getCloudService());
        
        if(builder.getMetaData() != null) {
            this.setMetaData(builder.getMetaData());
        }
        
        if(this.taskFactory == null) {
            this.taskFactory = new TaskFactoryImpl();
        }

    }


    @Override
    public void run() throws IOException {

        String status = null;
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        
        VmMetaData metaData = this.getMetaData();
        CloudService cloudService = this.getCloudService();

        while(true) {
            try {
                // only process tasks if the status is empty
                if(StringUtils.isBlank(status)) {

                    // set status to BUSY
                    metaData.setProcessorStatus(ProcessorStatus.BUSY);
                    cloudService.updateMetadata(metaData);

                    // run the task
                    Task task = taskFactory.getTask(metaData, cloudService);
                    if(task != null) {
                        stopwatch.start();
                        log.info("Starting processor task: " + task);
                        
                        task.run();
                        
                        log.info("TIMER# Task " + task + " completed in: " + stopwatch);
                        stopwatch.reset();

                    } else {
                        //no task is set, just set status to ready and wait for tasks
                        log.info("No task is set!");
                    }

                    // finished processing
                    // blank the task type and set the status to READY
                    metaData.clearValues();
                    metaData.setProcessorStatus(ProcessorStatus.READY);

                    cloudService.updateMetadata(metaData);

                } else {
                    log.info("will continue waiting for instructions as status is currently: " + status);
                }

                // now wait for any change in the metadata
                log.info("Waiting for new instructions from the Coordinator");

                // avoid race condition
                ApiUtils.block(2);
                metaData = cloudService.getMetaData(false);
                // if we still have a status then wait, otherwise proceed
                if(StringUtils.isNotBlank(metaData.getStatus())) {
                    metaData = cloudService.getMetaData(true);
                }

                // check the status in the metadata
                status = metaData.getStatus();

            } catch(Exception e) {

                log.error("An error has occurred whilst running/waiting for tasks, setting status to ERROR", e);
                // try to update the Metadata to a fail status
                try {

                    metaData = cloudService.getMetaData(false);
                    // blank the task type and set the status to ERROR
                    metaData.clearValues();
                    metaData.exceptionToCloudExError(e);
                    cloudService.updateMetadata(metaData);

                    // wait until we get further instructions
                    // now wait for any change in the metadata
                    log.info("Waiting for new instructions from the Coordinator");
                    metaData = cloudService.getMetaData(true);
                    status = metaData.getStatus();

                } catch(Exception e1) {
                    // all has failed with no hope of recovery, retry a few times then terminate
                    log.fatal("An error has occurred whilst trying to recover", e);
                    // self terminate :-(
                    // FIXME uncomment once testing is thoroughly done
                    //this.service.shutdownInstance();
                }
            }
            
            if(this.stop) {
                break;
            }
        }

    }


    /**
     * Set to true to stop this processor if it's already running
     * @param stop the stop to set
     */
    public final void setStop(boolean stop) {
        this.stop = stop;
    }


    /**
     * Builder for {@link Coordinator}
     * <p>
     * Implementation is not thread-safe.
     * </p>
     */
    public static final class Builder {

        private VmMetaData metaData;

        private CloudService cloudService;

        private TaskFactory taskFactory;

        /**
         * @param metaData - the metadata of the current vm
         * @param cloudService - the cloud service implementation
         */
        public Builder(CloudService cloudService) {
            super();
            this.cloudService = cloudService;
        }

        /**
         * Build a new instance of {@link Coordinator}
         * @return coordinator instance
         * @throws IOException if cloud api calls fail 
         */
        public Processor build() throws IOException {
            return new Processor(this);
        }

        /**
         * @param metaData - the metadata of the current vm
         */
        public Builder setMetaData(VmMetaData metaData) {
            this.metaData = metaData;
            return this;
        }

        /**
         * @param cloudService the cloudService to set
         */
        public Builder setCloudService(CloudService cloudService) {
            this.cloudService = cloudService;
            return this;
        }

        /**
         * @param taskFactory the taskFactory to set
         */
        public Builder setTaskFactory(TaskFactory taskFactory) {
            this.taskFactory = taskFactory;
            return this;
        }

        /**
         * @return the metaData
         */
        public final VmMetaData getMetaData() {
            return metaData;
        }

        /**
         * @return the cloudService
         */
        public final CloudService getCloudService() {
            return cloudService;
        }

        /**
         * @return the taskFactory
         */
        public final TaskFactory getTaskFactory() {
            return taskFactory;
        }

    }


}
