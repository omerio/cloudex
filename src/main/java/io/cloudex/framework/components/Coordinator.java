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
import io.cloudex.framework.cloud.CloudService;
import io.cloudex.framework.cloud.VmConfig;
import io.cloudex.framework.cloud.VmMetaData;
import io.cloudex.framework.config.Job;
import io.cloudex.framework.config.PartitionConfig;
import io.cloudex.framework.config.TaskConfig;
import io.cloudex.framework.exceptions.ClassInstantiationException;
import io.cloudex.framework.exceptions.InstancePopulationException;
import io.cloudex.framework.partition.PartitionFunction;
import io.cloudex.framework.partition.entities.Partition;
import io.cloudex.framework.partition.factory.PartitionFunctionFactory;
import io.cloudex.framework.partition.factory.PartitionFunctionFactoryImpl;
import io.cloudex.framework.task.Task;
import io.cloudex.framework.task.factory.TaskFactory;
import io.cloudex.framework.task.factory.TaskFactoryImpl;
import io.cloudex.framework.types.ErrorAction;
import io.cloudex.framework.types.PartitionType;
import io.cloudex.framework.types.TargetType;
import io.cloudex.framework.utils.Constants;
import io.cloudex.framework.utils.FileUtils;
import io.cloudex.framework.utils.ObjectUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * The CloudEx Coordinator Component
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Coordinator extends CommonExecutable {

    private static final Log log = LogFactory.getLog(Coordinator.class);

    // Tasks execution context
    private Context context;

    // processor names
    private Set<String> processors = new HashSet<>();

    private Job job;

    private PartitionFunctionFactory partitionFunctionFactory;

    private TaskFactory taskFactory;


    /**
     * 
     * @param job - the coordinator job
     * @param metaData - the vm meta data
     * @param cloudService - the cloud service implementation
     * @throws IOException if any of the cloud api calls fail
     */
    public Coordinator(Job job, VmMetaData metaData, CloudService cloudService) throws IOException {
        this(new Builder(job, cloudService));
    }

    /**
     * 
     * @param builder Coordinator builder
     * @throws IOException if any of the cloud api calls fail
     */
    Coordinator(Builder builder) throws IOException {
        super();

        Validate.notNull(builder.getCloudService(), "cloudService is required");

        this.job = builder.getJob();
        this.partitionFunctionFactory = builder.getPartitionFunctionFactory();
        this.taskFactory = builder.getTaskFactory();
        this.setCloudService(builder.getCloudService());

        Validate.notNull(this.job, "job must be provided");
        Validate.isTrue(this.job.isValid(), "job is not valid"); 

        if(builder.getMetaData() != null) {
            this.setMetaData(builder.getMetaData());
        }

        // create defaults
        if(this.partitionFunctionFactory == null) {
            this.partitionFunctionFactory = new PartitionFunctionFactoryImpl() ;
        }

        if(this.taskFactory == null) {
            this.taskFactory = new TaskFactoryImpl();
        }

        this.context = new Context(this.job.getData());
    }


    @Override
    public void run() throws IOException {

        Stopwatch stopwatch = Stopwatch.createStarted();

        List<TaskConfig> tasksConfig = job.getTasks();

        try {

            for(TaskConfig taskConfig: tasksConfig) {

                Stopwatch stopwatch1 = Stopwatch.createStarted();

                Set<String> outputKeys = taskConfig.getOutput();

                Task task = taskFactory.getTask(taskConfig, context, getCloudService());

                if(TargetType.PROCESSOR.equals(taskConfig.getTarget())) {

                    this.runProcessorTask(task, taskConfig);

                } else {

                    // run the task
                    if(ErrorAction.CONTINUE.equals(taskConfig.getErrorAction())) {
                        this.runTaskContinue(task);

                    } else {
                        task.run(); 
                    }

                    // get the output of the task
                    this.addTaskOutputToContext(task, outputKeys);

                }

                stopwatch1.stop();
                log.info("TIMER: Task " + getTaskName(task) + " completed in " + stopwatch);

            }

        } catch(Exception e) {
            log.error("An error has occurred", e);
            throw new IOException(e);

        } finally {
            // shutdown the processors
        }

        stopwatch.stop();
        log.info("TIMER: Job completed in " + stopwatch);

    }
    
    /**
     * Get the items that we will use for partition the work between the processors
     * @param task - the current task
     * @param partitionConfig - the parition config for the task
     * @param itemsKey - the key for the partition items, this is specified in the partitionConfig
     * @return Collection partition items
     * @throws ClassInstantiationException if the population of the bean fails
     * @throws InstancePopulationException if the population of the bean fails
     */
    @SuppressWarnings("unchecked")
    private Collection<String> getPartitionItems(Task task, PartitionConfig partitionConfig, String itemsKey) 
            throws ClassInstantiationException, InstancePopulationException {
        
        PartitionType partitionType = partitionConfig.getType();
        String output = partitionConfig.getOutput();
        
        Collection<String> items = null;
        
        if(PartitionType.ITEMS.equals(partitionType)) {

            Object value = this.context.resolveValue(itemsKey);

            Validate.notNull(value, "partition items are null for task " + getTaskName(task));

            if(!(value instanceof Collection)) {
                throw new IllegalArgumentException("Expecting partition items of type Collection, found: " + value);
            }

            items = (Collection<String>) value;

            if(items.isEmpty()) {
                throw new IllegalArgumentException("empty partition items for task " + getTaskName(task));
            }

        } else if(PartitionType.FUNCTION.equals(partitionType)) {

            PartitionFunction partitionFunction = this.partitionFunctionFactory.getPartitionFunction(
                    partitionConfig, context);
            List<Partition> partitions = partitionFunction.partition();
            if(partitions == null || partitions.isEmpty()) {
                throw new IllegalArgumentException("empty partitions for task " + getTaskName(task));
            }

            items = Partition.joinPartitionItems(partitions);

            this.context.put(output, items);

            itemsKey = Context.getKeyReference(output);
        }
        
        return items;
    }

    /**
     * Create and start a task on a number of processors
     * @param task
     * @param taskConfig
     * @throws ClassInstantiationException if the population of the bean fails
     * @throws InstancePopulationException if the population of the bean fails
     * @throws IOException if cloud api calls fail
     */
    private void runProcessorTask(Task task, TaskConfig taskConfig) throws ClassInstantiationException, 
        InstancePopulationException, IOException {
        
        Map<String, String> vmConfig = this.job.getVmConfig();
        // check the parition function
        PartitionConfig partitionConfig = taskConfig.getPartitioning();
        Map<String, String> partitionInput = partitionConfig.getInput();
        String itemsKey = partitionInput.get(PartitionFunction.ITEMS_KEY);
        Validate.notNull(itemsKey, "partition items key is required");
        
        // bucket just in case we have long metadata
        String bucket = (String) this.getJobDataValue(Constants.CLOUD_STORAGE_BUCKET_KEY);
        String zoneId = (String) this.getJobDataValue(Constants.CLOUD_ZONE_ID);
          
        Map<String, String> processorInput = new HashMap<>(taskConfig.getInput());
        
        Entry<String, String> partitionItemEntry = null;
        for(Entry<String, String> entry: processorInput.entrySet()) {
            String value = entry.getValue();
            if(value.equals(itemsKey)) {
                partitionItemEntry = entry;
                break;
            }
        }
        
        if(partitionItemEntry == null) {
            throw new IllegalArgumentException("expecting an input key/value for partition item in task " 
                    + getTaskName(task));
        }
        
        String itemMetaDataKey = partitionItemEntry.getKey();
        
        // remove the partition item key/value from the input mapping
        processorInput.remove(itemMetaDataKey);
        
        List<String> activeProcessors = Lists.newArrayList(this.processors);
        List<String> newProcessors = new ArrayList<>();
        
        List<VmConfig> vmsConfig = new ArrayList<>();
        long timestamp = (new Date()).getTime();
        
        Collection<String> items = this.getPartitionItems(task, partitionConfig, itemsKey);
        
        int numberOfProcessors = items.size();
        
        int count = 0;
        int index = 0;
        
        for(String item: items) {
            
            // add the metadata
            VmMetaData metaData = new VmMetaData();
            
            for(Entry<String, String> entry: processorInput.entrySet()) {
                String value = (String) this.context.resolveValue(entry.getValue());
                metaData.addUserValue(entry.getKey(), value);
            }
            
            // add the item, first check if it's too long
            if(item.length() > this.getCloudService().getMaximumMetaDataSize()) {
                
                Validate.notBlank(bucket, "bucket is needed as metadata is larger than maximum allowed.");
                
                log.info("Metadata too large, using file. Size = " + item.length());
                // use files instead
                String itemFilename = new StringBuilder(itemMetaDataKey).append('_').append(timestamp)
                        .append('_').append(index).append(Constants.DOT_TEXT).toString();  
                
                String itemFile = FileUtils.TEMP_FOLDER + itemFilename;
                // save to file
                FileUtils.objectToJsonFile(itemFile, ObjectUtils.csvToSet(item));
                
                // upload the file to cloud storage
                this.getCloudService().uploadFileToCloudStorage(itemFile, bucket);
                // update the meta data with the file value
                metaData.addUserValue(itemMetaDataKey + VmMetaData.LONG_METADATA_FILE_Suffix, itemFilename);
                
                index++;
            
            } else {
                
                metaData.addUserValue(itemMetaDataKey, item);
            }
            
            // re-program existing processors
            if(activeProcessors.size() > 0) {
                
                String instanceId = activeProcessors.iterator().next();
                activeProcessors.remove(activeProcessors.indexOf(instanceId));
                VmMetaData processorMetaData = this.getCloudService().getMetaData(instanceId, zoneId);
                processorMetaData.getFollowUp(metaData);
                this.getCloudService().updateMetadata(metaData, zoneId, instanceId, true);
                newProcessors.add(instanceId);
                
             
            } else {
                // start new VMs
                
                String instanceId = VmMetaData.CLOUDEX_VM_PREFIX + (timestamp + count);
                VmConfig conf = new VmConfig();
                ObjectUtils.populate(conf, vmConfig);
                conf.setInstanceId(instanceId);
                conf.setMetaData(metaData);

                newProcessors.add(instanceId);
                                
                vmsConfig.add(conf);
                
                log.info("Create VM Config for " + instanceId);
                count++;
            }
            
        }
           
    }

    /**
     * Run a task capturing IOException
     * @param task
     */
    private void runTaskContinue(Task task) {
        try {
            task.run();

        } catch(IOException ioe) {
            log.warn("Task: " +  getTaskName(task) + " has thrown an exception", ioe);
        }
    }

    private String getTaskName(Task task) {
        return task.getClass().getName();
    }

    /**
     * Add the out of coordinator task execution to the context
     * @param task
     * @param outputKeys
     */
    private void addTaskOutputToContext(Task task, Set<String> outputKeys) {
        Map<String, Object> output = task.getOutput();

        if(outputKeys != null) {
            for(String key: outputKeys) {
                if(output.containsKey(key)) {
                    this.context.put(key, output.get(key));
                }
            }
        }
    }

    /**
     * Get an item from the job data
     * @param key
     * @return
     */
    private Object getJobDataValue(String key) {
        return this.job.getData().get(key);
    }

    /**
     * Builder for {@link Coordinator}
     * <p>
     * Implementation is not thread-safe.
     * </p>
     */
    public static final class Builder {

        private Job job;

        private VmMetaData metaData;

        private CloudService cloudService;

        private PartitionFunctionFactory partitionFunctionFactory;

        private TaskFactory taskFactory;

        /**
         * @param job - the job to execute
         * @param cloudService - the cloud service implementation
         */
        public Builder(Job job, CloudService cloudService) {
            super();
            this.job = job;
            this.cloudService = cloudService;
        }

        /**
         * Build a new instance of {@link Coordinator}
         * @return coordinator instance
         * @throws IOException 
         */
        public Coordinator build() throws IOException {
            return new Coordinator(this);
        }

        /**
         * @param job the job to set
         */
        public Builder setJob(Job job) {
            this.job = job;
            return this;
        }

        /**
         * @param metaData the metaData to set
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
         * @param partitionFunctionFactory the partitionFunctionFactory to set
         */
        public Builder setPartitionFunctionFactory(
                PartitionFunctionFactory partitionFunctionFactory) {
            this.partitionFunctionFactory = partitionFunctionFactory;
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
         * @return the job
         */
        public final Job getJob() {
            return job;
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
         * @return the partitionFunctionFactory
         */
        public final PartitionFunctionFactory getPartitionFunctionFactory() {
            return partitionFunctionFactory;
        }

        /**
         * @return the taskFactory
         */
        public final TaskFactory getTaskFactory() {
            return taskFactory;
        }

    }

}
