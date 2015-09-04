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
import io.cloudex.framework.config.Job;
import io.cloudex.framework.config.PartitionConfig;
import io.cloudex.framework.config.TaskConfig;
import io.cloudex.framework.config.VmConfig;
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
import io.cloudex.framework.types.ProcessorStatus;
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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * The CloudEx Coordinator Component
 * TODO add processor vm object with start/end times and vm config (stats + config)
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Coordinator extends CommonExecutable {

    private static final Log log = LogFactory.getLog(Coordinator.class);
    
    private static final String NO_TASK = "No Task";

    // Tasks execution context
    private Context context;

    // processor names
    private Set<String> processors = new HashSet<>();

    private Job job;

    private PartitionFunctionFactory partitionFunctionFactory;

    private TaskFactory taskFactory;
    
    private boolean shutdownProcessors;


    /**
     * 
     * @param job - the coordinator job
     * @param metaData - the vm meta data
     * @param cloudService - the cloud service implementation
     * @throws IOException if any of the cloud api calls fail
     */
    public Coordinator(Job job, CloudService cloudService) throws IOException {
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
        this.shutdownProcessors = builder.isShutdownProcessors();

        Validate.notNull(this.job, "job must be provided");
        
        if(!this.job.valid()) {
            List<String> messages = this.job.getValidationErrors();
            log.error("Job is not valid: " + messages);
            throw new IllegalArgumentException("Job is not valid: " + messages);
        }

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
        this.context.putReadOnly(Constants.PRCESSORS_KEY, this.processors);
    }


    /*
     * (non-Javadoc)
     * @see io.cloudex.framework.Executable#run()
     */
    @Override
    public void run() throws IOException {

        Stopwatch stopwatch = Stopwatch.createStarted();

        List<TaskConfig> tasksConfig = job.getTasks();

        try {

            for(TaskConfig taskConfig: tasksConfig) {

                Stopwatch stopwatch1 = Stopwatch.createStarted();

                Set<String> outputKeys = taskConfig.getOutput();

                String taskName;

                if(TargetType.PROCESSOR.equals(taskConfig.getTarget())) {

                    this.runProcessorTask(taskConfig);
                    taskName = this.getTaskName(taskConfig);

                } else {
                    
                    Task task = taskFactory.getTask(taskConfig, context, getCloudService());

                    // run the task
                    if(ErrorAction.CONTINUE.equals(taskConfig.getErrorAction())) {
                        this.runTaskContinue(task);

                    } else {
                        task.run(); 
                    }

                    // get the output of the task
                    this.addTaskOutputToContext(task, outputKeys);
                    taskName = this.getTaskName(task);

                }

                stopwatch1.stop();
                log.info("TIMER: Task " + taskName + " completed in " + stopwatch);

            }

        } catch(Exception e) {
            log.error("An error has occurred", e);
            
            if(e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e);
            }

        } finally {
            // shutdown the processors
            if(this.shutdownProcessors) {
                log.info("Shutting down all processors");
                this.shutdownProcessors();
            }
        }

        stopwatch.stop();
        log.info("TIMER: Job completed in " + stopwatch);

    }
    
    /**
     * Shutdown all the procesors
     */
    protected void shutdownProcessors() {
        // shutdown active nodes
        List<VmConfig> configs = new ArrayList<>();
        for(String processorId: this.processors) {
            VmConfig config = new VmConfig();
            config.setInstanceId(processorId);
            configs.add(config);
        }
        
        try {
            this.getCloudService().shutdownInstance(configs);
            this.processors.clear();
            
        } catch (IOException e) {
            log.error("Failed to shutdown processors", e);
        }
    }


    /**
     * Create and start a task on a number of processors
     * @param task - the task to start
     * @param taskConfig - the task config
     * @throws ClassInstantiationException if the population of the bean fails
     * @throws InstancePopulationException if the population of the bean fails
     * @throws IOException if cloud api calls fail
     */
    private void runProcessorTask(TaskConfig taskConfig) throws ClassInstantiationException, 
        InstancePopulationException, IOException {

        final CloudService cloudService = this.getCloudService();
        
        VmConfig vmConfig = this.job.getVmConfig();
        // check the parition function
        PartitionConfig partitionConfig = taskConfig.getPartitioning();
        Map<String, String> partitionInput = partitionConfig.getInput();
        String itemsKey = partitionInput.get(PartitionFunction.ITEMS_KEY);
        
        // for partition function the items key is the output key of the function
        if(PartitionType.FUNCTION.equals(partitionConfig.getType())) {
            String output = partitionConfig.getOutput();
            itemsKey = Context.getKeyReference(output);
        }
        
        Validate.notNull(itemsKey, "partition items key is required");

        // bucket just in case we have long metadata
        String bucket = (String) this.getJobDataValue(Constants.CLOUD_STORAGE_BUCKET_KEY);
        String zoneId = this.job.getVmConfig().getZoneId();

        List<String> idleProcessors = Lists.newArrayList(this.processors);
        List<String> busyProcessors = new ArrayList<>();

        List<VmConfig> vmsConfig = new ArrayList<>();
        long timestamp = (new Date()).getTime();

        int count = 0;
        int index = 0;
        
        int metaDataMaxSize = cloudService.getMaximumMetaDataSize();
        
        Map<String, String> processorInput = new HashMap<>();
        Entry<String, String> partitionItemEntry = null;

        if(taskConfig.getInput() != null) {
            processorInput.putAll(taskConfig.getInput());

            for(Entry<String, String> entry: processorInput.entrySet()) {
                String value = entry.getValue();
                if(value.equals(itemsKey)) {
                    partitionItemEntry = entry;
                    break;
                }
            }
        }
        
        // for some processor tasks we might not need to give them any thing from the partition item.
        // for example if this task doesn't need any input
        String itemMetaDataKey = null;

        if(partitionItemEntry != null) {
            //throw new IllegalArgumentException("expecting an input key/value for partition item in task " 
              //      + getTaskName(taskConfig));           
            itemMetaDataKey = partitionItemEntry.getKey();
            // remove the partition item key/value from the input mapping
            processorInput.remove(itemMetaDataKey);

        }
        
        // get all the resolved task inputs
        Map<String, String> resolvedInputs = new HashMap<>();
        for(Entry<String, String> entry: processorInput.entrySet()) {
            String value = (String) this.context.resolveValue(entry.getValue());
            resolvedInputs.put(entry.getKey(), value);
        }
        
        Collection<String> items = this.getPartitionItems(taskConfig, partitionConfig, itemsKey);

        for(String item: items) {

            // add the metadata
            VmMetaData metaData = new VmMetaData();
            metaData.setTaskClass(taskConfig.getClassName());
            
            // add the task inputs
            metaData.addUserValues(resolvedInputs);
            
            // do we need to inject the task with the item?
            if(itemMetaDataKey != null) {
                
                // add the item, first check if it's too long
                if(item.length() > metaDataMaxSize) {

                    this.saveMetaDataItemToFile(metaData, item, itemMetaDataKey, bucket, index, timestamp);

                    index++;

                } else {

                    metaData.addUserValue(itemMetaDataKey, item);
                }
            }

            // re-program existing processors
            if(idleProcessors.size() > 0) {

                String instanceId = idleProcessors.iterator().next();
                idleProcessors.remove(idleProcessors.indexOf(instanceId));
                VmMetaData processorMetaData = cloudService.getMetaData(instanceId, zoneId);
                processorMetaData.getFollowUp(metaData);
                cloudService.updateMetadata(metaData, zoneId, instanceId, true);
                busyProcessors.add(instanceId);


            } else {
                // start new VMs

                String instanceId = VmMetaData.CLOUDEX_VM_PREFIX + (timestamp + count);
                busyProcessors.add(instanceId);

                VmConfig conf = vmConfig.copy();
                conf.setInstanceId(instanceId);
                conf.setMetaData(metaData);

                vmsConfig.add(conf);

                log.info("Create VM Config for " + instanceId);
                count++;
            }

        }
        
        this.processors.addAll(busyProcessors);

        if(!vmsConfig.isEmpty()) {
            boolean success = cloudService.startInstance(vmsConfig, true);
            if(!success) {
                throw new IOException("Some processors have failed to start");
                // TODO better error handling and retry
            }
        }

        IOException processorException = this.waitForProcessors(busyProcessors, zoneId);

        // if any of the nodes has failed then throw an exception
        if((processorException != null) && ErrorAction.EXIT.equals(taskConfig.getErrorAction())) {
            // for now we are throwing an exception, in the future need to return a status so tasks can be retried
            throw processorException;
        }

        log.info("Successfully completed processor task " + this.getTaskName(taskConfig));
        log.info("Number of idle processors: " + this.processors.size() + ", instance Ids: " + this.processors);

    }
    
    /**
     * Save the meta data for the provide metaData item into a file, then reference it
     * @param metaData - metaData object
     * @param item - the string item to save
     * @param itemMetaDataKey - the item metadata key to use
     * @param bucket - the cloud bucket to use for storing the file
     * @param index - the current index
     * @param timestamp - the timestamp
     * @throws IOException if cloud api calls fail
     */
    private void saveMetaDataItemToFile(VmMetaData metaData, String item, String itemMetaDataKey, String bucket, 
            int index, long timestamp) throws IOException {
        
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
    }
    
    /**
     * Wait for all the processors to complete return exceptions if they are thrown by the processors
     * @param processors - the processors
     * @param zoneId - the cloud zoneId
     * @return IOException if any of the processors throws it
     * @throws IOException if the cloud api calls fail
     */
    private IOException waitForProcessors(List<String> processors, String zoneId) throws IOException {
        IOException processorException = null;

        // wait for the VMs to finish their loading
        for(String instanceId: processors) {   
            boolean ready = false;

            do {
                ApiUtils.block(this.getCloudService().getApiRecheckDelay());

                VmMetaData metaData = this.getCloudService().getMetaData(instanceId, zoneId);
                ready = ProcessorStatus.READY.equals(metaData.getProcessorStatus());
                // check for ERROR status
                if(ProcessorStatus.ERROR.equals(metaData.getProcessorStatus())) {
                    processorException = ApiUtils.exceptionFromCloudExError(metaData, instanceId);
                    log.error(instanceId + " processor has failed", processorException);
                    break;
                }

            } while (!ready);
        }

        return processorException;
    }

    /**
     * Get the items that we will use for partition the work between the processors. If a partition function
     * is used then the output of the partition function is added to the context.
     * @param taskConfig - the current task config
     * @param partitionConfig - the parition config for the task
     * @param itemsKey - the key for the partition items, this is specified in the partitionConfig
     * @return Collection partition items
     * @throws ClassInstantiationException if the population of the bean fails
     * @throws InstancePopulationException if the population of the bean fails
     */
    @SuppressWarnings("unchecked")
    private Collection<String> getPartitionItems(TaskConfig taskConfig, PartitionConfig partitionConfig, 
            String itemsKey) throws ClassInstantiationException, InstancePopulationException {

        PartitionType partitionType = partitionConfig.getType();

        Collection<String> items = null;

        if(PartitionType.ITEMS.equals(partitionType)) {

            Object value = this.context.resolveValue(itemsKey);

            Validate.notNull(value, "partition items are null or empty for task " + getTaskName(taskConfig));

            if(!(value instanceof Collection)) {
                throw new IllegalArgumentException("Expecting partition items of type Collection, found: " + value);
            }

            items = (Collection<String>) value;

            if(items.isEmpty()) {
                throw new IllegalArgumentException("empty partition items for task " + getTaskName(taskConfig));
            }
            
            // cast an element to a string
            if(!(items.iterator().next() instanceof String)) {
                throw new IllegalArgumentException("partition items must be a collection of strings");
            }

        } else if(PartitionType.FUNCTION.equals(partitionType)) {

            PartitionFunction partitionFunction = this.partitionFunctionFactory.getPartitionFunction(
                    partitionConfig, context);
            List<Partition> partitions = partitionFunction.partition();
            if(partitions == null || partitions.isEmpty()) {
                throw new IllegalArgumentException("empty partitions for task " + getTaskName(taskConfig));
            }

            items = Partition.joinPartitionItems(partitions);
            
            // add the output of the partition function to the context
            String output = partitionConfig.getOutput();
            this.context.put(output, items);

        }

        return items;
    }


    /**
     * Run a task capturing IOException
     * @param task - the task to run
     */
    private void runTaskContinue(Task task) {
        try {
            task.run();

        } catch(IOException ioe) {
            log.warn("Task: " +  getTaskName(task) + " has thrown an exception", ioe);
        }
    }

    /**
     * Get task name from a task
     * @param task - the task
     * @return - display name
     */
    private String getTaskName(Task task) {
        return (task != null) ? task.getClass().getName() : NO_TASK;
    }
    
    /**
     * Get task name from a task config
     * @param taskConfig - the task config
     * @return display name
     */
    private String getTaskName(TaskConfig taskConfig) {
        String name = NO_TASK;
        if(taskConfig != null) {
            name = taskConfig.getClassName();
        }
        return name;
    }

    /**
     * Add the out of coordinator task execution to the context
     * @param task - the completed task
     * @param outputKeys - the keys to use to query the task output
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
     * @param key - the key
     * @return the value
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
        
        private boolean shutdownProcessors;

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
         * @throws IOException if cloud api calls fail
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

        /**
         * @return the shutdownProcessors
         */
        public final boolean isShutdownProcessors() {
            return shutdownProcessors;
        }

        /**
         * @param shutdownProcessors the shutdownProcessors to set
         */
        public final Builder setShutdownProcessors(boolean shutdownProcessors) {
            this.shutdownProcessors = shutdownProcessors;
            return this;
        }

    }

    /**
     * @return the context
     */
    protected final Context getContext() {
        return context;
    }

    /**
     * @return the processors
     */
    protected final Set<String> getProcessors() {
        return processors;
    }

    /**
     * @return the job
     */
    protected final Job getJob() {
        return job;
    }

    /**
     * @return the partitionFunctionFactory
     */
    protected final PartitionFunctionFactory getPartitionFunctionFactory() {
        return partitionFunctionFactory;
    }

    /**
     * @return the taskFactory
     */
    protected final TaskFactory getTaskFactory() {
        return taskFactory;
    }

    /**
     * @return the shutdownProcessors
     */
    protected final boolean isShutdownProcessors() {
        return shutdownProcessors;
    }

}
