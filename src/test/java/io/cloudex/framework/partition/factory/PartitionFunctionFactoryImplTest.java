package io.cloudex.framework.partition.factory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.cloudex.framework.components.Context;
import io.cloudex.framework.config.PartitionConfig;
import io.cloudex.framework.exceptions.ClassInstantiationException;
import io.cloudex.framework.exceptions.InstancePopulationException;
import io.cloudex.framework.partition.PartitionFunction;
import io.cloudex.framework.partition.builtin.BinPackingPartitionTest;
import io.cloudex.framework.partition.builtin.BuiltInPartitionFunctions;
import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.partition.entities.Partition;
import io.cloudex.framework.types.PartitionType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class PartitionFunctionFactoryImplTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testGetPartitionFunction() throws ClassInstantiationException, InstancePopulationException {
        
        Context context = new Context();
        context.put("maxItems", 10L);
        context.put("newPercent", 0.0d);
        List<Item> items = BinPackingPartitionTest.createItems();
        context.put(PartitionFunction.ITEMS_KEY, items);
        
        // test function class name
        PartitionConfig config = getConfig();
        
        PartitionFunction function = (new PartitionFunctionFactoryImpl()).getPartitionFunction(config, context);
        
        List<Partition> partitions = function.partition();
        assertNotNull(partitions);
        assertEquals(5, partitions.size());
        
        // test function name
        config.setClassName(null);
        config.setFunctionName(BuiltInPartitionFunctions.BinPackingPartition.toString());
        function = (new PartitionFunctionFactoryImpl()).getPartitionFunction(config, context);
        
        partitions = function.partition();
        assertNotNull(partitions);
        assertEquals(5, partitions.size());
       
    }
    
    @Test(expected = NullPointerException.class)
    public void testGetPartitionFunctionFailItemsMissing() throws ClassInstantiationException, 
        InstancePopulationException {
        Context context = new Context();
        context.put("maxItems", 10L);
        context.put("newPercent", 0.0d);
        PartitionConfig config = getConfig();
        
        (new PartitionFunctionFactoryImpl()).getPartitionFunction(config, context);
    }
    
    @Test(expected = InstancePopulationException.class)
    public void testGetPartitionFunctionFailInput() throws ClassInstantiationException, InstancePopulationException {
        Context context = new Context();
        context.put("maxItems", 10L);
        context.put("newPercent", 0.0d);
        List<Item> items = BinPackingPartitionTest.createItems();
        context.put(PartitionFunction.ITEMS_KEY, items);
        PartitionConfig config = getConfig();
        config.getInput().put("test", "somevalue");
        
        (new PartitionFunctionFactoryImpl()).getPartitionFunction(config, context);
    }
    
    @Test(expected = ClassCastException.class)
    public void testGetPartitionFunctionFailItemsInvalid() throws ClassInstantiationException, 
        InstancePopulationException {
        Context context = new Context();
        context.put("maxItems", 10L);
        context.put("newPercent", 0.0d);
        List<String> items = Lists.newArrayList("hello", "goodbye");
        context.put(PartitionFunction.ITEMS_KEY, items);
        PartitionConfig config = getConfig();
        
        (new PartitionFunctionFactoryImpl()).getPartitionFunction(config, context).partition();
    }
    
    @Test(expected = ClassInstantiationException.class)
    public void testGetPartitionFunctionFailClassInvalid() throws ClassInstantiationException, 
        InstancePopulationException {
        Context context = new Context();
        context.put("maxItems", 10L);
        context.put("newPercent", 0.0d);
        List<Item> items = BinPackingPartitionTest.createItems();
        context.put(PartitionFunction.ITEMS_KEY, items);
        PartitionConfig config = getConfig();
        config.setClassName("io.cloudex.framework.task.CommonTask");
        (new PartitionFunctionFactoryImpl()).getPartitionFunction(config, context).partition();
    }
    
    
    private PartitionConfig getConfig() {
        PartitionConfig config = new PartitionConfig();
        //config.setFunctionName("TestFunction");
        config.setType(PartitionType.FUNCTION);
        
        config.setOutput("test");
        config.setClassName("io.cloudex.framework.partition.builtin.BinPackingPartition");
        
        Map<String, String> input = new HashMap<>();
        input.put(PartitionFunction.ITEMS_KEY, "#items");
        input.put("maxBinItems", "#maxItems");
        input.put("newBinPercentage", "#newPercent");
        
        config.setInput(input);
        return config;
    }

}
