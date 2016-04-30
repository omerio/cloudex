package io.cloudex.framework.components.tasks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.cloudex.framework.components.ProcessorTest;
import io.cloudex.framework.task.CommonTask;

import java.io.IOException;


public class ProcessorFakeTask extends CommonTask {

    private String bucket;

    private String schema;



    /**
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }



    /**
     * @param bucket the bucket to set
     */
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }



    /**
     * @return the schema
     */
    public String getSchema() {
        return schema;
    }



    /**
     * @param schema the schema to set
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }



    @Override
    public void run() throws IOException {
        
        // check injected values aren't null
        System.out.println("running fake task");
        assertNotNull(bucket, "bucket is null");
        assertNotNull(schema, "schema is null");
        
        if("Error".equals(bucket)) {
            throw new IOException("Task has failed");
        } 
        
        assertEquals(ProcessorTest.BUCKET_VALUE, bucket);
        assertEquals(ProcessorTest.SCHEMA_VALUE, schema);
        
        this.cloudService.createCloudStorageBucket(bucket, "mylocation");
        
    }

}