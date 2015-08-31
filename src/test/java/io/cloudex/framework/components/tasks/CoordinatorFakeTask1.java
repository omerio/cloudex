package io.cloudex.framework.components.tasks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.cloudex.framework.components.CoordinatorRunCoordinatorTaskTest;
import io.cloudex.framework.task.CommonTask;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;


public class CoordinatorFakeTask1 extends CommonTask {

    private String bucket;

    private String schema;
    
    private boolean modify;
    
    private Collection<String> processors;



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
        
        assertNotNull(processors);
        assertEquals(3, processors.size());
        
        if(modify) {
            // should throw an exception
            processors.add("something");
        }
        
        // set the output values
        this.output.put(CoordinatorRunCoordinatorTaskTest.SCHEMA_TERMS_FILE_KEY, CoordinatorRunCoordinatorTaskTest.SCHEMA_TERMS_FILE_VALUE);
        
        // add some other output
        this.output.put("somekey", "undeclared_value");
    }



    /**
     * @return the modify
     */
    public boolean isModify() {
        return modify;
    }



    /**
     * @param modify the modify to set
     */
    public void setModify(boolean modify) {
        this.modify = modify;
    }



    /**
     * @return the processors
     */
    public Collection<String> getProcessors() {
        return processors;
    }



    /**
     * @param processors the processors to set
     */
    public void setProcessors(Collection<String> processors) {
        this.processors = processors;
    }

}