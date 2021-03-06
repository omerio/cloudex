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
import io.cloudex.framework.cloud.entities.VmInstance;
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
import java.net.SocketTimeoutException;
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
 * The CloudEx Coordinator Component. Processor tasks can be run on VM configuration that is 
 * different from the ones used for the job. These VMs will be shutdown as soon as the task
 * is completed and won't be added to the VM pool for reuse.
 * 
 * This coordinator keeps track processor vms stats, such as start/end times, cost and vm config.
 * 
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

    private Map<String, VmInstance> processorInstances = new HashMap<>();

    private Job job;

    private PartitionFunctionFactory partitionFunctionFactory;

    private TaskFactory taskFactory;

    private boolean shutdownProcessors;


    /**
     * 
     * @param job - the coordinator job
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

                log.info("TIMER# Job elapsed time: " + stopwatch);

                if(TargetType.PROCESSOR.equals(taskConfig.getTarget())) {

                    taskName = this.getTaskName(taskConfig);
                    log.info("Starting processor task: " + taskName);

                    this.runProcessorTask(taskConfig);

                } else {

                    Task task = taskFactory.getTask(taskConfig, context, getCloudService());
                    taskName = this.getTaskName(task);

                    log.info("Starting coordinator task: " + taskName);

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
                log.info("TIMER# Task " + taskName + " completed in " + stopwatch1);
                log.info("Total processors usage cost: " + this.calculateProcessorsCost());

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

            log.info("Total processors usage cost: " + this.calculateProcessorsCost());
        }

        log.debug("Coordinator's context: " + this.context);

        stopwatch.stop();
        log.info("TIMER# Job completed in " + stopwatch);

    }

    /**
     * Calculate the cost of usage of the processor VMs
     * @return - the total cost
     */
    public double calculateProcessorsCost() {
        double cost = 0.0;
        for(VmInstance instance: this.processorInstances.values()) {
            cost += instance.getCost();
        }

        return cost;
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
            //this.processors.clear();

        } catch (IOException e) {
            log.error("Failed to shutdown processors", e);

        } finally {

            // mark all VmInstances as shutdown
            this.updateVmInstances(configs, true);
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

        // do we need to start custom vms for this task?
        VmConfig taskVmConfig = taskConfig.getVmConfig();
        String taskVmConfigRef = taskConfig.getVmConfigReference();
        
        if((taskVmConfig == null) && (taskVmConfigRef != null)) {
            taskVmConfig = (VmConfig) this.context.resolveValue(taskVmConfigRef);
            Validate.notNull(taskVmConfig, "Unable to find VmConfig in job context with reference: " + taskVmConfigRef);
            taskConfig.setVmConfig(taskVmConfig);
        }
        
        final boolean taskUsesCustomVms = (taskVmConfig != null);

        // check the parition function
        PartitionConfig partitionConfig = taskConfig.getPartitioning();
        Map<String, String> partitionInput = partitionConfig.getInput();
        String itemsKey = null;

        if(partitionInput != null) {
            itemsKey = partitionInput.get(PartitionFunction.ITEMS_KEY);
        }

        // for partition function the items key is the output key of the function
        if(PartitionType.FUNCTION.equals(partitionConfig.getType())) {
            String output = partitionConfig.getOutput();
            itemsKey = Context.getKeyReference(output);
        }

        if(!PartitionType.COUNT.equals(partitionConfig.getType())) {
            Validate.notNull(itemsKey, "partition items key is required");
        }

        // bucket just in case we have long metadata
        String bucket = (String) this.getJobDataValue(Constants.CLOUD_STORAGE_BUCKET_KEY);
        String zoneId = this.job.getVmConfig().getZoneId();

        List<String> idleProcessors;

        if(taskUsesCustomVms) {
            idleProcessors = Lists.newArrayList();
            log.debug("Task: " + this.getTaskName(taskConfig) + " uses custom vm config: " + taskConfig.getVmConfig());

            taskVmConfig = vmConfig.merge(taskConfig.getVmConfig());
            // do we have any existing config that matches what this task need?

            for(String instanceId: this.processorInstances.keySet()) {
                VmInstance instance = this.processorInstances.get(instanceId);
                if(taskVmConfig.equals(instance.getVmConfig())) {
                    log.debug("Found existing processor that matches task requirements: " + instance.getVmConfig());
                    idleProcessors.add(instanceId);
                }
            }

        } else {
            idleProcessors = Lists.newArrayList(this.processors);
        }

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

            if(itemsKey != null) {
                for(Entry<String, String> entry: processorInput.entrySet()) {
                    String value = entry.getValue();
                    if(value.equals(itemsKey)) {
                        partitionItemEntry = entry;
                        break;
                    }
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
        
        List<String> operations = new ArrayList<>();

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
                operations.add(cloudService.updateMetadata(metaData, zoneId, instanceId, false));
                busyProcessors.add(instanceId);


            } else {
                // start new VMs

                String instanceId = VmMetaData.CLOUDEX_VM_PREFIX + (timestamp + count);
                busyProcessors.add(instanceId);

                VmConfig conf;

                if(taskUsesCustomVms) {
                    // if the task uses a custom vm then merge the config with the job vm config
                    conf = taskVmConfig;

                } else {
                    conf = vmConfig.copy();
                }

                conf.setInstanceId(instanceId);
                conf.setMetaData(metaData);

                vmsConfig.add(conf);

                log.info("Create VM Config for " + instanceId);
                count++;
            }

        }
        
        // wait for the metadata operations to complete
        if(!operations.isEmpty()) {
            this.waitForOperations(operations, zoneId);
        }

        if(!taskUsesCustomVms || (taskUsesCustomVms && !Boolean.FALSE.equals(taskConfig.getVmConfig().getReuse()))) {
            this.processors.addAll(busyProcessors);
        }

        if(!vmsConfig.isEmpty()) {
            boolean success = cloudService.startInstance(vmsConfig, true);
            if(!success) {
                throw new IOException("Some processors have failed to start");
                // TODO better error handling and retry
            }

            this.updateVmInstances(vmsConfig, false);
        }

        IOException processorException = this.waitForProcessors(busyProcessors, zoneId);

        // if any of the nodes has failed then throw an exception
        if((processorException != null) && ErrorAction.EXIT.equals(taskConfig.getErrorAction())) {
            // for now we are throwing an exception, in the future need to return a status so tasks can be retried
            throw processorException;
        }

        if(taskUsesCustomVms && Boolean.FALSE.equals(taskConfig.getVmConfig().getReuse())) {
            // shutdown the custom vms
            log.debug("Shutting down custom vms: " + vmsConfig + " for task: " + this.getTaskName(taskConfig));
            this.getCloudService().shutdownInstance(vmsConfig);
            // just in case if we have this vm in the processors then remove it

            this.updateVmInstances(vmsConfig, true);
        }

        log.info("Successfully completed processor task " + this.getTaskName(taskConfig));
        log.info("Number of idle processors: " + this.processors.size() + ", instance Ids: " + this.processors);

    }

    /**
     * Update the VmInstances with start & end date
     * @param configs - the VmConfigs of started/ended processors
     * @param setEnd - true to update the end date on the VmInstance
     */
    private void updateVmInstances(List<VmConfig> configs, boolean setEnd) {

        Date now = new Date();

        for(VmConfig config: configs) {
            String instanceId = config.getInstanceId();
            VmInstance instance = this.processorInstances.get(instanceId);
            if(instance == null) {
                instance = new VmInstance(config, now);
                this.processorInstances.put(instanceId, instance);
            }

            if(setEnd) {
                instance.setEnd(now);
                this.processors.remove(instanceId);
            }
        }
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
     * Wait for the cloud operations to complete
     * @param operations - the reference of the operations to check
     * @param zoneId - the cloud provider zoneId
     * @throws IOException IOException if the cloud api calls fail
     */
    private void waitForOperations(List<String> operations, String zoneId) throws IOException {
        boolean success = false;
        int retries = 0;
        // block and wait for the meta data operations to complete
        do {
            try {
                this.getCloudService().blockOnComputeOperations(operations, zoneId);
                
                success = true;
                
                retries = 0;
                
            } catch(IOException e) {
                
                log.error("Exception whilst waiting for operations completion", e);
                
                // retry 3 times
                if(retries == 3) {
                    throw e;
                    
                } else {
                    
                    retries++;
                }
            }
            
        } while(success == false);
        
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
        
        int retries = 0;

        // wait for the VMs to finish their loading
        for(String instanceId: processors) {   
            boolean ready = false;
            // TODO this code waits for one processor until it's available then moves to the next
            // need to restructure it such that each processor failure is reported immediately
            
            // TODO add timeout, in case one processor crashes
            do {
                ApiUtils.block(this.getCloudService().getApiRecheckDelay());
                try {
                    VmMetaData metaData = this.getCloudService().getMetaData(instanceId, zoneId);
                    ready = ProcessorStatus.READY.equals(metaData.getProcessorStatus());
                    // check for ERROR status
                    if(ProcessorStatus.ERROR.equals(metaData.getProcessorStatus())) {
                        processorException = ApiUtils.exceptionFromCloudExError(metaData, instanceId);
                        log.error(instanceId + " processor has failed", processorException);
                        break;
                    }
                    
                    retries = 0;
                    
                } catch(SocketTimeoutException e) {
                    log.warn("Timeout exception whilst waiting for processor metadata update", e);
                
                } catch(IOException e) {
                    
                    log.error("An exception occurred whilst waiting for processors", e);
                    // retry 3 times
                    if(retries == 3) {
                        throw e;
                    } else {
                        
                        retries++;
                    }
                }

            } while (!ready);
        }

        return processorException;
    }

    /**
     * Get the items that we will use for partition the work between the processors. If a partition function
     * is used then the output of the partition function is added to the context.
     * @param taskConfig - the current task config
     * @param partitionConfig - the partition config for the task
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

        switch(partitionType) {

            case COUNT:
                int count = 0;
                
                if(partitionConfig.getCount() != null) {
                    count = partitionConfig.getCount();
                
                } else {
                    
                    Object value = this.context.resolveValue(partitionConfig.getCountRef());
                    
                    Validate.notNull(value, "count reference is null or empty for task " + getTaskName(taskConfig));
        
                    if(!(value instanceof Double)) {
                        throw new IllegalArgumentException("Expecting count reference of numeric type, found: " 
                                + value);
                    }
        
                    count = ((Double) value).intValue();
                    
                }
                
                items = new HashSet<>();
                for(int i = 0; i < count; i++) {
                    items.add("Item" + i);
                }
                break;
    
            case FUNCTION:
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
    
                break;
    
            case ITEMS:
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
                break;
                
            default:
                throw new IllegalArgumentException("Invalid PartitionType: " + partitionType);
                
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
     * @return the processorInstances
     */
    protected Map<String, VmInstance> getProcessorInstances() {
        return processorInstances;
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
