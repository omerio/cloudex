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


package io.cloudex.framework.components;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.cloud.entities.StorageObject;
import io.cloudex.framework.cloud.entities.VmMetaData;
import io.cloudex.framework.config.Job;
import io.cloudex.framework.config.JobTest;
import io.cloudex.framework.config.TaskConfig;
import io.cloudex.framework.config.VmConfig;
import io.cloudex.framework.exceptions.ProcessorException;
import io.cloudex.framework.partition.builtin.BinPackingPartitionTest;
import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.partition.entities.Partition;
import io.cloudex.framework.types.ErrorAction;
import io.cloudex.framework.types.ProcessorStatus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Sets;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
@RunWith(JMockit.class)
public class CoordinatorRunProcessorTaskTest {

    public static final String SCHEMA_TERMS_FILE_VALUE = "schema_terms_file.json";
    public static final String SCHEMA_TERMS_FILE_KEY = "schemaTermsFile";

    public static final String BUCKET_KEY = "bucket";
    public static final String BUCKET_VALUE = "testBucket";

    public static final String SCHEMA_KEY = "schema";
    public static final String SCHEMA_VALUE = "myschemafile.txt";

    private VmMetaData metaData;

    // private CloudService cloudService;

    private List<Item> items;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        metaData = new VmMetaData();
        items = BinPackingPartitionTest.createItems();
    }

    /**
     * Test method for {@link io.cloudex.framework.components.Coordinator#run()}.
     * @throws IOException 
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testTaskPartitionFunction() throws IOException {

        Job job = getJob("CoordinatorTest1.json");
        final CloudService service = getCloudService(job.getVmConfig(), 5).getMockInstance();
        Coordinator coordinator = new Coordinator(job, service);        
        Context context = populateContext(coordinator);
        coordinator.run();
        assertEquals(5, coordinator.getProcessors().size());

        List<Partition> partitions = (List<Partition>) context.get("filePartitions");
        assertNotNull(partitions);

        assertEquals(5, partitions.size());
    }
    
    /**
     * Test method for {@link io.cloudex.framework.components.Coordinator#run()}.
     * @throws IOException 
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testTaskPartitionFunctionNoPartitionItem() throws IOException {

        Job job = getJob("CoordinatorTest4.json");
        final CloudService service =  getCloudService(true, 1000, job.getVmConfig(), 5, 
                false, false, new HashSet<String>()).getMockInstance();
        
        Coordinator coordinator = new Coordinator(job, service);        
        Context context = populateContext(coordinator);
        coordinator.run();
        assertEquals(5, coordinator.getProcessors().size());

        List<Partition> partitions = (List<Partition>) context.get("filePartitions");
        assertNotNull(partitions);

        assertEquals(5, partitions.size());
        
    }
    
    /**
     * Test method for {@link io.cloudex.framework.components.Coordinator#run()}.
     * @throws IOException 
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testTaskPartitionItemsNoTaskInput() throws IOException {

        Job job = getJob("CoordinatorTest5.json");
        
        final Set<String> processors = Sets.newHashSet("processor1", "processor2", "processor3", "processor4");
        
        final MockUp<CloudService> mockup = new MockUp<CloudService>() {

            @Mock(invocations = 1)
            public VmMetaData init() throws IOException { 
                return metaData; 
            }

            @Mock(invocations = 1)
            public int getMaximumMetaDataSize() { 
                return 10000; 
            }

            @Mock(invocations = 0)
            public boolean startInstance(List<VmConfig> configs, boolean block) throws IOException {
                return true;
            }

            @Mock(invocations = 8)
            public VmMetaData getMetaData(String instanceId, String zoneId) throws IOException {
                System.out.println("Getting metadata for instance: " + instanceId);
                assertNotNull(instanceId);
                //ApiUtils.block(1);
                VmMetaData metaData = new VmMetaData();
                metaData.setProcessorStatus(ProcessorStatus.READY);

                return metaData;
            }
            
            @Mock(invocations = 0)
            public void updateMetadata(VmMetaData metaData) throws IOException { 
            }
            
            @Mock(invocations = 4)
            public void updateMetadata(VmMetaData metaData, 
                    String zoneId, String instanceId, boolean block) throws IOException {
                assertNotNull(zoneId);
                assertTrue(processors.contains(instanceId));
                assertTrue(block);
                assertNotNull(metaData);
                assertNotNull(metaData.getTaskClass());
                assertTrue(metaData.getUserMetaData().isEmpty());
            }

            @Mock(minInvocations = 1)
            public int getApiRecheckDelay() { 
                return 0; 
            }
            
            @Mock(invocations = 0)
            public StorageObject uploadFileToCloudStorage(String filename, String bucket) throws IOException {
                return null;
            }

            @Mock(invocations = 0)
            public void shutdownInstance(List<VmConfig> configs) throws IOException {
            }

        };
        final CloudService service =  mockup.getMockInstance();
        
        Coordinator coordinator = new Coordinator(job, service);   
        coordinator.getProcessors().addAll(processors);
        Context context = populateContext(coordinator);
        Collection<String> processorsLive = (Collection<String>) context.resolveValue("#processors");
        assertNotNull(processorsLive);
        assertEquals(4, processorsLive.size());
        coordinator.run();
        assertEquals(4, coordinator.getProcessors().size());
        
    }

    /**
     * shutdown after the processor is finished
     * @throws IOException
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testTaskPartitionFunctionWithShutdown() throws IOException {
        //Coordinator coordinator = new Coordinator.Builder(job, service).setShutdownProcessors(true).build(); 

        Job job = getJob("CoordinatorTest1.json");
        Set<String> metaDataKeys = job.getTasks().get(0).getInput().keySet();
        final CloudService service = 
                getCloudService(true, 1000, job.getVmConfig(), 5, true, false, metaDataKeys).getMockInstance();
        Coordinator coordinator = new Coordinator.Builder(job, service).setShutdownProcessors(true).build();        
        final Context context = populateContext(coordinator);
        
        coordinator.run();
        
        assertEquals(0, coordinator.getProcessors().size());

        List<Partition> partitions = (List<Partition>) context.get("filePartitions");
        assertNotNull(partitions);

        assertEquals(5, partitions.size());

    }

    /**
     * meta data is too large and hence stored in a file
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    public void testTaskSuccessLargeMetadata() throws FileNotFoundException, IOException {
        //
        Job job = getJob("CoordinatorTest1.json");
        Set<String> metaDataKeys = Sets.newHashSet(job.getTasks().get(0).getInput().keySet());
        
        metaDataKeys.remove("files");
        metaDataKeys.add("files" + VmMetaData.LONG_METADATA_FILE_Suffix);
        
        final CloudService service = 
                getCloudService(true, 0, job.getVmConfig(), 5, true, false, metaDataKeys).getMockInstance();
        Coordinator coordinator = new Coordinator(job, service);        
        Context context = populateContext(coordinator);
        coordinator.run();
        assertEquals(5, coordinator.getProcessors().size());

        List<Partition> partitions = (List<Partition>) context.get("filePartitions");
        assertNotNull(partitions);

        assertEquals(5, partitions.size());
    }

    /**
     * a processor throws an exception
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    @Test(expected = ProcessorException.class)
    public void testTaskProcessorException() throws FileNotFoundException, IOException {
        
        Job job = getJob("CoordinatorTest1.json");
        Set<String> metaDataKeys = job.getTasks().get(0).getInput().keySet();
        final CloudService service = 
                getCloudService(true, 1000, job.getVmConfig(), 5, true, true, metaDataKeys).getMockInstance();
        Coordinator coordinator = new Coordinator(job, service);     
        final Context context = populateContext(coordinator);
        
        coordinator.run();
        
    }
    
    /**
     * a processor throws an exception
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    @Test
    public void testTaskProcessorExceptionContinue() throws FileNotFoundException, IOException {
        
        Job job = getJob("CoordinatorTest1.json");
        TaskConfig task =  job.getTasks().get(0);
        task.setErrorAction(ErrorAction.CONTINUE);
        Set<String> metaDataKeys = task.getInput().keySet();
        final CloudService service = 
                getCloudService(true, 1000, job.getVmConfig(), 5, true, true, metaDataKeys).getMockInstance();
        Coordinator coordinator = new Coordinator(job, service);     
        final Context context = populateContext(coordinator);
        
        coordinator.run();
        
        assertEquals(5, coordinator.getProcessors().size());

        List<Partition> partitions = (List<Partition>) context.get("filePartitions");
        assertNotNull(partitions);

        assertEquals(5, partitions.size());
        
    }

    /**
     * uses partition items instead of function
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    @Test
    public void testTaskPartitionItems() throws FileNotFoundException, IOException {
        
        Job job = getJob("CoordinatorTest2.json");
        final CloudService service = getCloudService(job.getVmConfig(), 3).getMockInstance();
        Coordinator coordinator = new Coordinator(job, service);        
        Context context = populateContext(coordinator);
        Set<String> items = Sets.newHashSet("key1", "key2", "key3");
        context.put("fileItems", items);
        coordinator.run();
        assertEquals(3, coordinator.getProcessors().size());

    }

    //full & partial running processors, 2 already running and 3 new processors
    @Test
    public void testTaskWithPartialRunningProcessors() throws FileNotFoundException, IOException {

        Job job = getJob("CoordinatorTest1.json");
        final VmConfig config = job.getVmConfig();
        TaskConfig task =  job.getTasks().get(0);
        final Set<String> metaDataKeys = task.getInput().keySet();
        
        final MockUp<CloudService> mockup = new MockUp<CloudService>() {

            @Mock(invocations = 1)
            public VmMetaData init() throws IOException { 
                return metaData; 
            }

            @Mock(invocations = 1)
            public int getMaximumMetaDataSize() { 
                return 1000; 
            }

            @Mock(invocations = 1)
            public boolean startInstance(List<VmConfig> configs, boolean block) throws IOException {


                assertNotNull(configs);
                assertEquals(3, configs.size());
                assertTrue(block);

                for(VmConfig conf: configs) {
                    assertEquals(config.getDiskType(), conf.getDiskType());
                    assertEquals(config.getImageId(), conf.getImageId());
                    assertEquals(config.getNetworkId(), conf.getNetworkId());
                    assertEquals(config.getStartupScript(), conf.getStartupScript());
                    assertEquals(config.getVmType(), conf.getVmType());
                    assertEquals(config.getZoneId(), conf.getZoneId());
                    System.out.println("Starting processor with id: " + conf.getInstanceId());
                    assertNotNull(conf.getInstanceId());
                    assertNotNull(conf.getMetaData());

                    VmMetaData metaData = conf.getMetaData();

                    assertNotNull(metaData.getTaskClass());

                    if(metaDataKeys != null) {
                        Map<String, String> userData = metaData.getUserMetaData();

                        for(String key: metaDataKeys) {
                            assertNotNull("Missing key in metadata: " + key, userData.get(key));
                        }
                    }
                }

                return true;
            }

            @Mock(invocations = 7)
            public VmMetaData getMetaData(String instanceId, String zoneId) throws IOException {
                System.out.println("Getting metadata for instance: " + instanceId);
                assertNotNull(instanceId);
                //ApiUtils.block(1);
                VmMetaData metaData = new VmMetaData();
                metaData.setProcessorStatus(ProcessorStatus.READY);
                return metaData;
            }
            
            @Mock(invocations = 2)
            public void updateMetadata(VmMetaData metaData, String zoneId, String instanceId, boolean block) throws IOException {
                System.out.println("Updating metadata for instance: " + instanceId);
                assertNotNull(instanceId);
                assertNotNull(zoneId);
                assertTrue(block);
                assertNotNull(metaData.getTaskClass());

                if(metaDataKeys != null) {
                    Map<String, String> userData = metaData.getUserMetaData();

                    for(String key: metaDataKeys) {
                        assertNotNull("Missing key in metadata: " + key, userData.get(key));
                    }
                    
                    for(String key: userData.keySet()) {
                        assertTrue("Key in metadata, but not in metaData keys to check " + key, metaDataKeys.contains(key));
                    }
                }
            }
            
            
            @Mock(minInvocations = 1)
            public int getApiRecheckDelay() { 
                return 0; 
            }
            
        };
        
        final CloudService service = mockup.getMockInstance();
        Coordinator coordinator = new Coordinator(job, service);        
        final Context context = populateContext(coordinator);
        coordinator.getProcessors().add("existing_processor1");
        coordinator.getProcessors().add("existing_processor2");
        
        coordinator.run();
        assertEquals(5, coordinator.getProcessors().size());
        List<Partition> partitions = (List<Partition>) context.get("filePartitions");
        
        assertNotNull(partitions);

        assertEquals(5, partitions.size());
        
        
    }
    
    
    @Test
    public void testTaskWithAllRunningProcessors() throws FileNotFoundException, IOException {

        Job job = getJob("CoordinatorTest1.json");
        final VmConfig config = job.getVmConfig();
        TaskConfig task =  job.getTasks().get(0);
        final Set<String> metaDataKeys = task.getInput().keySet();
        
        final MockUp<CloudService> mockup = new MockUp<CloudService>() {

            @Mock(invocations = 1)
            public VmMetaData init() throws IOException { 
                return metaData; 
            }

            @Mock(invocations = 1)
            public int getMaximumMetaDataSize() { 
                return 1000; 
            }

            @Mock(invocations = 0)
            public boolean startInstance(List<VmConfig> configs, boolean block) throws IOException {
                return false;
            }

            @Mock(invocations = 10)
            public VmMetaData getMetaData(String instanceId, String zoneId) throws IOException {
                System.out.println("Getting metadata for instance: " + instanceId);
                assertNotNull(instanceId);
                //ApiUtils.block(1);
                VmMetaData metaData = new VmMetaData();
                metaData.setProcessorStatus(ProcessorStatus.READY);
                return metaData;
            }
            
            @Mock(invocations = 5)
            public void updateMetadata(VmMetaData metaData, String zoneId, String instanceId, boolean block) 
                    throws IOException {
                
                System.out.println("Updating metadata for instance: " + instanceId);
                assertNotNull(instanceId);
                assertNotNull(zoneId);
                assertTrue(block);
                assertNotNull(metaData.getTaskClass());

                if(metaDataKeys != null) {
                    Map<String, String> userData = metaData.getUserMetaData();

                    for(String key: metaDataKeys) {
                        assertNotNull("Missing key in metadata: " + key, userData.get(key));
                    }
                }
            }
            
            
            @Mock(minInvocations = 1)
            public int getApiRecheckDelay() { 
                return 0; 
            }
            
        };
        
        final CloudService service = mockup.getMockInstance();
        Coordinator coordinator = new Coordinator(job, service);        
        final Context context = populateContext(coordinator);
        coordinator.getProcessors().add("existing_processor1");
        coordinator.getProcessors().add("existing_processor2");
        coordinator.getProcessors().add("existing_processor3");
        coordinator.getProcessors().add("existing_processor4");
        coordinator.getProcessors().add("existing_processor5");
        
        coordinator.run();
        assertEquals(5, coordinator.getProcessors().size());
        List<Partition> partitions = (List<Partition>) context.get("filePartitions");
        
        assertNotNull(partitions);

        assertEquals(5, partitions.size());
        
        
    }

    private Context populateContext(Coordinator coordinator) {
        Context context = coordinator.getContext();
        assertNotNull(context);
        context.put("fileItems", items);
        context.put("schemaTermsFile", "someFilename.json");

        return context;
    }

    private MockUp<CloudService> getCloudService(VmConfig config, int numOfProcessors) {
        return getCloudService(true, 1000, config, numOfProcessors, false, false, null);
    }

    private Job getJob(String filename) throws FileNotFoundException, IOException {
        Job job = JobTest.loadJob(filename);
        assertNotNull(job);
        assertNotNull(job.getTasks());
        assertEquals(1, job.getTasks().size());
        System.out.println(job.getValidationErrors());
        assertTrue(job.valid()); 
        return job;
    }

    /**
     * Get a cloud service mockup
     * @return
     */
    private MockUp<CloudService> getCloudService(final boolean startInstanceSuccess, 
            final int maximumMetaDataSize, final VmConfig config, final int numOfProcessors,
            final boolean shutdown, final boolean processorError, final Set<String> metaDataKeys) {

        final MockUp<CloudService> mockup = new MockUp<CloudService>() {

            @Mock(invocations = 1)
            public VmMetaData init() throws IOException { 
                return metaData; 
            }

            @Mock(invocations = 1)
            public int getMaximumMetaDataSize() { 
                return maximumMetaDataSize; 
            }

            @Mock(invocations = 1)
            public boolean startInstance(List<VmConfig> configs, boolean block) throws IOException {


                assertNotNull(configs);
                assertEquals(numOfProcessors, configs.size());
                assertTrue(block);

                for(VmConfig conf: configs) {
                    assertEquals(config.getDiskType(), conf.getDiskType());
                    assertEquals(config.getImageId(), conf.getImageId());
                    assertEquals(config.getNetworkId(), conf.getNetworkId());
                    assertEquals(config.getStartupScript(), conf.getStartupScript());
                    assertEquals(config.getVmType(), conf.getVmType());
                    assertEquals(config.getZoneId(), conf.getZoneId());
                    System.out.println("Starting processor with id: " + conf.getInstanceId());
                    assertNotNull(conf.getInstanceId());
                    assertNotNull(conf.getMetaData());

                    VmMetaData metaData = conf.getMetaData();

                    assertNotNull(metaData.getTaskClass());

                    if(metaDataKeys != null) {
                        Map<String, String> userData = metaData.getUserMetaData();

                        for(String key: metaDataKeys) {
                            assertNotNull("Missing key in metadata: " + key, userData.get(key));
                        }
                    }
                }

                return startInstanceSuccess;
            }

            @Mock(minInvocations = 1)
            public VmMetaData getMetaData(String instanceId, String zoneId) throws IOException {
                System.out.println("Getting metadata for instance: " + instanceId);
                assertNotNull(instanceId);
                //ApiUtils.block(1);
                VmMetaData metaData = new VmMetaData();

                if(processorError) {

                    metaData.exceptionToCloudExError(new IOException("Processor has failed"));

                } else {
                    metaData.setProcessorStatus(ProcessorStatus.READY);
                }

                return metaData;
            }

            @Mock(minInvocations = 1)
            public int getApiRecheckDelay() { 
                return 0; 
            }
            
            @Mock
            public StorageObject uploadFileToCloudStorage(String filename, String bucket) throws IOException {
                System.out.println("Uploading file: " + filename + ", to cloud bucket: " + bucket);
                assertNotNull(bucket);
                assertNotNull(filename); 
                return null;
            }

            @Mock
            public void shutdownInstance(List<VmConfig> configs) throws IOException {
                if(!shutdown) {
                    throw new IOException("Unexpected call to CloudService.shutdownInstance");
                }
                
                System.out.println("Shutting down instances: " + configs);
                
                assertNotNull(configs);
                assertEquals(5, configs.size());

                for(VmConfig config: configs) {
                    assertNotNull(config.getInstanceId());
                }
            }

        };



        return mockup;
    }


}
