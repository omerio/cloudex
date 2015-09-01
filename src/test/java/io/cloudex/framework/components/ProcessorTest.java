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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import io.cloudex.framework.cloud.CloudService;
import io.cloudex.framework.cloud.VmMetaData;
import io.cloudex.framework.cloud.api.ApiUtils;
import io.cloudex.framework.exceptions.ClassInstantiationException;
import io.cloudex.framework.exceptions.InstancePopulationException;
import io.cloudex.framework.task.factory.TaskFactory;
import io.cloudex.framework.task.factory.TaskFactoryImpl;
import io.cloudex.framework.types.ProcessorStatus;

import java.io.IOException;
import java.util.Map;

import mockit.Mock;
import mockit.MockUp;
import mockit.Verifications;
import mockit.integration.junit4.JMockit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
@RunWith(JMockit.class)
public class ProcessorTest {

    public static final String BUCKET_KEY = "bucket";
    public static final String BUCKET_VALUE = "testBucket";

    public static final String SCHEMA_KEY = "schema";
    public static final String SCHEMA_VALUE = "myschemafile.txt";

    private VmMetaData metaData;

    private Processor processor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        metaData = new VmMetaData();
    }

    @Test
    public void testCreateSuccess() throws IOException {
        final MockUp<CloudService> mockup = new MockUp<CloudService>() {
            @Mock(invocations = 1)
            public VmMetaData init() throws IOException { 
                return metaData; 
            }
        };
        Processor processor = new Processor(mockup.getMockInstance());
        assertNotNull(processor.getMetaData());
    }

    /**
     * Test method for {@link io.cloudex.framework.components.Processor#run()}.
     * @throws IOException 
     * @throws InstancePopulationException 
     * @throws ClassInstantiationException 
     */
    @Test
    public void testRun() throws IOException, ClassInstantiationException, InstancePopulationException {
        final CloudService service = getCloudService().getMockInstance();
        final TaskFactory taskFactory = new TaskFactoryImpl();
        processor = new Processor.Builder(service).setTaskFactory(taskFactory).build();

        processor.setStop(true);
        populateMetaData();

        assertNull(metaData.getProcessorStatus());
        processor.run();

        ProcessorStatus status = metaData.getProcessorStatus();

        assertNotNull(status);
        assertEquals(ProcessorStatus.READY, status);

        new Verifications() {
            {
                taskFactory.getTask(metaData, service);
            }
        };

    }

    /**
     * processor task fails
     * @throws IOException
     * @throws ClassInstantiationException
     * @throws InstancePopulationException
     */
    @Test
    public void testRunError() throws IOException, ClassInstantiationException, InstancePopulationException {
        final MockUp<CloudService> mockup = new MockUp<CloudService>() {

            private int metaDataUpdates;

            @Mock(invocations = 1)
            public VmMetaData init() throws IOException { 
                return metaData; 
            }

            @Mock(minInvocations = 2)
            public VmMetaData getMetaData(boolean waitForChange) throws IOException {

                if(waitForChange) {

                    // simulate a short wait
                    ApiUtils.block(4);

                }

                return metaData;
            }

            @Mock(invocations = 2)
            public void updateMetadata(VmMetaData metaData) throws IOException {

                metaDataUpdates++;

                assertNotNull(metaData);

                ProcessorStatus status = metaData.getProcessorStatus();
                assertNotNull(status);

                if(metaDataUpdates == 1) {
                    // status should be busy
                    assertEquals(ProcessorStatus.BUSY, status);
                    assertTrue(metaData.getAttributes().size() > 1);

                } else if(metaDataUpdates == 2) {

                    assertEquals(ProcessorStatus.ERROR, status);
                    assertEquals(3, metaData.getAttributes().size());

                }
            }

            @Mock(invocations = 0)
            public void createCloudStorageBucket(String bucket, String location) throws IOException {

            }

        };

        final CloudService service = mockup.getMockInstance();
        final TaskFactory taskFactory = new TaskFactoryImpl();
        processor = new Processor.Builder(service).setTaskFactory(taskFactory).build();

        processor.setStop(true);
        populateMetaData();
        this.metaData.addUserValue(BUCKET_KEY, "Error");

        assertNull(metaData.getProcessorStatus());
        processor.run();

        ProcessorStatus status = metaData.getProcessorStatus();

        assertNotNull(status);
        assertEquals(ProcessorStatus.ERROR, status);
        assertNotNull(metaData.getException());
        assertNotNull(metaData.getMessage());

        new Verifications() {
            {
                taskFactory.getTask(metaData, service);
            }
        };

    }

    @Test
    public void testRunTwice() throws IOException, ClassInstantiationException, InstancePopulationException {
        final MockUp<CloudService> mockup = new MockUp<CloudService>() {

            private int metaDataUpdates;
            private int metaDataRead;

            @Mock(invocations = 1)
            public VmMetaData init() throws IOException { 
                return metaData; 
            }

            @Mock(invocations = 4)
            public VmMetaData getMetaData(boolean waitForChange) throws IOException {

                metaDataRead++;

                if(waitForChange) {

                    // simulate a short wait
                    ApiUtils.block(4);

                }

                if(metaDataRead == 2) {
                    metaData.clearValues();
                    populateMetaData();
                }

                return metaData;
            }

            @Mock(invocations = 4)
            public void updateMetadata(VmMetaData metaData) throws IOException {

                metaDataUpdates++;

                assertNotNull(metaData);

                ProcessorStatus status = metaData.getProcessorStatus();
                assertNotNull(status);

                if(metaDataUpdates % 2 == 0) {
                    // status should be ready
                    assertEquals(ProcessorStatus.READY, status);
                    assertEquals(1, metaData.getAttributes().size());

                } else {

                    // status should be busy
                    assertEquals(ProcessorStatus.BUSY, status);
                    assertTrue(metaData.getAttributes().size() > 1);
                }

                if(metaDataUpdates == 4) {
                    processor.setStop(true);
                }
            }

            @Mock(invocations = 2)
            public void createCloudStorageBucket(String bucket, String location) throws IOException {
                assertNotNull(bucket);
                assertNotNull(location);

                assertEquals(BUCKET_VALUE, bucket);
                assertEquals("mylocation", location);

                ApiUtils.block(3);
            }

        };

        final CloudService service = mockup.getMockInstance();
        final TaskFactory taskFactory = new TaskFactoryImpl();
        processor = new Processor.Builder(service).setTaskFactory(taskFactory).build();

        populateMetaData();

        assertNull(metaData.getProcessorStatus());
        processor.run();

        ProcessorStatus status = metaData.getProcessorStatus();

        assertNotNull(status);
        assertEquals(ProcessorStatus.READY, status);

        new Verifications() {
            {
                taskFactory.getTask(metaData, service);
            }
        };

    }

    private final void populateMetaData() {
        metaData.addUserValue(BUCKET_KEY, BUCKET_VALUE);
        metaData.addUserValue(SCHEMA_KEY, SCHEMA_VALUE);

        metaData.setTaskClass("io.cloudex.framework.components.tasks.ProcessorFakeTask");

    }

    private MockUp<CloudService> getCloudService() {

        final MockUp<CloudService> mockup = new MockUp<CloudService>() {

            private int metaDataUpdates;

            @Mock(invocations = 1)
            public VmMetaData init() throws IOException { 
                return metaData; 
            }

            @Mock(minInvocations = 2)
            public VmMetaData getMetaData(boolean waitForChange) throws IOException {

                if(waitForChange) {

                    // simulate a short wait
                    ApiUtils.block(4);

                }

                return metaData;
            }

            @Mock(invocations = 2)
            public void updateMetadata(VmMetaData metaData) throws IOException {

                metaDataUpdates++;

                assertNotNull(metaData);

                ProcessorStatus status = metaData.getProcessorStatus();
                assertNotNull(status);

                if(metaDataUpdates == 1) {
                    // status should be busy
                    assertEquals(ProcessorStatus.BUSY, status);
                    assertTrue(metaData.getAttributes().size() > 1);

                } else if(metaDataUpdates == 2) {

                    if(metaData.getException() != null) {
                        assertEquals(ProcessorStatus.ERROR, status);
                        assertEquals(3, metaData.getAttributes().size());

                    } else {
                        // status should be ready
                        assertEquals(ProcessorStatus.READY, status);
                        assertEquals(1, metaData.getAttributes().size());
                    }


                }
            }

            @Mock(invocations = 1)
            public void createCloudStorageBucket(String bucket, String location) throws IOException {
                assertNotNull(bucket);
                assertNotNull(location);

                assertEquals(BUCKET_VALUE, bucket);
                assertEquals("mylocation", location);

                ApiUtils.block(3);
            }

        };

        return mockup;
    }


}
