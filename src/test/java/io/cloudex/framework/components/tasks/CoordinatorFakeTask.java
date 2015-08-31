package io.cloudex.framework.components.tasks;

import io.cloudex.framework.components.CoordinatorRunCoordinatorTaskTest;
import io.cloudex.framework.task.CommonTask;
import static org.junit.Assert.*;

import java.io.IOException;


public class CoordinatorFakeTask extends CommonTask {

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
        
        assertEquals(CoordinatorRunCoordinatorTaskTest.BUCKET_VALUE, bucket);
        assertEquals(CoordinatorRunCoordinatorTaskTest.SCHEMA_VALUE, schema);
        
        // set the output values
        this.output.put(CoordinatorRunCoordinatorTaskTest.SCHEMA_TERMS_FILE_KEY, CoordinatorRunCoordinatorTaskTest.SCHEMA_TERMS_FILE_VALUE);
        
        // add some other output
        this.output.put("somekey", "undeclared_value");
    }

}