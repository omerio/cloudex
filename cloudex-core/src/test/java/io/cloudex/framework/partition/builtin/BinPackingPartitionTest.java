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
package io.cloudex.framework.partition.builtin;

import static org.junit.Assert.*;
import io.cloudex.framework.partition.PartitionUtils;
import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.partition.entities.Partition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class BinPackingPartitionTest {

    private static Map<Long, Double> SCALE1 = new HashMap<>();
    static {
        SCALE1.put(20L, 2.0D);
        SCALE1.put(25L, 3.0D);
        SCALE1.put(10L, 1.0D);
        SCALE1.put(8L, 1.0D);
        SCALE1.put(5L, 0.5D);
        SCALE1.put(2L, 0.5D);
        SCALE1.put(1L, 0.5D);
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link io.ecarf.core.partition.BinPackingPartition#partition()}.
     */
    @Test
    public void testPartitionLargestGTMax() {
        BinPackingPartition function = new BinPackingPartition(this.createItems()); 
        function.setMaxBinItems(10L);
        function.setNewBinPercentage(0.0);
        List<Partition> bins = function.partition();

        for(Partition bin: bins) {
            System.out.println(bin);

            for(Item item: bin.getItems()) {
                //System.out.println(item);
                assertEquals(SCALE1.get(item.getWeight()), item.getScale());
            }
        }
        //System.out.println(bins);
        assertEquals(5, bins.size());
    }

    @Test
    public void testPartitionEqual() {
        BinPackingPartition function = new BinPackingPartition(this.createItems(5L, 10)); 
        function.setMaxBinItems(10L);
        function.setNewBinPercentage(0.0);
        List<Partition> bins = function.partition();

        Double point5 = new Double(0.5);
        for(Partition bin: bins) {
            System.out.println(bin);

            for(Item item: bin.getItems()) {
                //System.out.println(item);
                assertEquals(point5, item.getScale());
            }
        }
        //System.out.println(bins);
        assertEquals(5, bins.size());

        // without a specified maximum
        function = new BinPackingPartition(this.createItems(5L, 10)); 
        function.setNewBinPercentage(0.0);
        bins = function.partition();

        Double one = new Double(1.0);
        for(Partition bin: bins) {
            System.out.println(bin);

            for(Item item: bin.getItems()) {
                //System.out.println(item);
                assertEquals(one, item.getScale());
            }
        }
        //System.out.println(bins);
        assertEquals(10, bins.size());
    }

    @Test
    public void testSetNumberOfBins() {
        BinPackingPartition function = new BinPackingPartition(this.createItems()); 
        function.setNumberOfBins(2);
        List<Partition> bins = function.partition();

        for(Partition bin: bins) {
            System.out.println(bin + ", sum: " + bin.sum());

            for(Item item: bin.getItems()) {
                System.out.println(item);
                //assertEquals(SCALE1.get(item.getWeight()), item.getScale());
            }
            
        }
        //System.out.println(bins);
        assertEquals(2, bins.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testItemScale() {
        List<Item> items = (List<Item>) PartitionUtils.setScale(createItems(), 10L);
        for(Item item: items) {
            System.out.println(item);
            assertEquals(SCALE1.get(item.getWeight()), item.getScale());
        }

    }
    
    @Test
    public void testSetBinCapacityApproximatelyEqualCapacity() {
        setBinCapacityApproximatelyEqualCapacity(32, 9, 226881280L);
        setBinCapacityApproximatelyEqualCapacity(1, 276, 7260200953L);
    }
    
    
    private void setBinCapacityApproximatelyEqualCapacity(int numBins, int maxItemsPerBin, long maxWeightPerBin) {
        BinPackingPartition function = new BinPackingPartition(this.createManyItems()); 
       
        
        function.setMaxBinItems(maxWeightPerBin);
        List<Partition> bins = function.partition();
        // unique files
        Set<String> files = new HashSet<>();
        // all files
        List<String> filesl = new ArrayList<>();
        long totalSum = 0;
        for(Partition bin: bins) {
            
            long sum = bin.sum();
            totalSum += sum;
            
            System.out.println("Bin , sum: " + sum + ", items: " + bin.getItems().size());

            for(Item item: bin.getItems()) {
                files.add(item.getKey());
                filesl.add(item.getKey());
                
            }
            
            assertTrue(bin.getItems().size() <= maxItemsPerBin);
        }
        
        // ensure number of bins
        assertEquals(numBins, bins.size());
        
        // ensure total sum
        assertEquals(7260200953L, totalSum);
        
        // ensure unique 267 files
        assertEquals(267, files.size());
        assertEquals(267, filesl.size());
        
        System.out.println("Ideal bin capacity: " + (totalSum / bins.size()));
        System.out.println("Total capacity: " + totalSum);
        
        
    }
    
    @Test
    public void testSetNumberOfBinsApproximatelyEqualCapacity() {
        setNumberOfBinsApproximatelyEqualCapacity(16, 18);
        setNumberOfBinsApproximatelyEqualCapacity(2, 135);
        setNumberOfBinsApproximatelyEqualCapacity(1, 267);
        setNumberOfBinsApproximatelyEqualCapacity(300, 267);
    }
    
    
    private void setNumberOfBinsApproximatelyEqualCapacity(int numBins, int maxItemsPerBin) {
        BinPackingPartition function = new BinPackingPartition(this.createManyItems()); 
        
        
        function.setNumberOfBins(numBins);
        List<Partition> bins = function.partition();
        // unique files
        Set<String> files = new HashSet<>();
        // all files
        List<String> filesl = new ArrayList<>();
        long totalSum = 0;
        for(Partition bin: bins) {
            
            long sum = bin.sum();
            totalSum += sum;
            
            System.out.println("Bin , sum: " + sum + ", items: " + bin.getItems().size());

            for(Item item: bin.getItems()) {
                files.add(item.getKey());
                filesl.add(item.getKey());
                
            }
            
            assertTrue(bin.getItems().size() <= maxItemsPerBin);
        }
        
        // ensure number of bins
        assertEquals(numBins, bins.size());
        
        // ensure total sum
        assertEquals(7260200953L, totalSum);
        
        // ensure unique 267 files
        assertEquals(267, files.size());
        assertEquals(267, filesl.size());
        
        System.out.println("Ideal bin capacity: " + (totalSum / numBins));
        System.out.println("Total capacity: " + totalSum);
        
        
    }

    /**
     * 
     * @return
     */
    public static List<Item> createItems() {

        // expected total bins 5
        List<Item> items = new ArrayList<>();

        Item item = new Item("key1", 20L);
        items.add(item);

        item = new Item("key2", 25L);
        items.add(item);

        item = new Item("key3", 10L);
        items.add(item);

        item = new Item("key4", 8L);
        items.add(item);

        item = new Item("key5", 5L);
        items.add(item);

        item = new Item("key6", 2L);
        items.add(item);

        item = new Item("key7", 1L);
        items.add(item);

        return items;
    }

    private List<Item> createItems(Long weight, Integer count) {
        List<Item> items = new ArrayList<>();

        for(int i = 0; i < count; i++) {
            items.add(new Item("key" + i, weight));
        }

        return items;
    }
    
    private List<Item> createManyItems() {
        List<Item> items = new ArrayList<>();
        
        items.add(new Item("univ_batch_0.nt.gz",  27081782L));
        items.add(new Item("univ_batch_1.nt.gz",  27404241L));
        items.add(new Item("univ_batch_2.nt.gz",  27109369L));
        items.add(new Item("univ_batch_8.nt.gz",  26855605L));
        items.add(new Item("univ_batch_9.nt.gz",  26882407L));
        items.add(new Item("univ_batch_7.nt.gz",  26785851L));
        items.add(new Item("univ_batch_6.nt.gz",  27311772L));
        items.add(new Item("univ_batch_5.nt.gz",  27393812L));
        items.add(new Item("univ_batch_4.nt.gz",  26548782L));
        items.add(new Item("univ_batch_3.nt.gz",  27142767L));
        items.add(new Item("univ_batch_20.nt.gz", 27072052L));
        items.add(new Item("univ_batch_10.nt.gz", 27868925L));
        items.add(new Item("univ_batch_11.nt.gz", 27401607L));
        items.add(new Item("univ_batch_12.nt.gz", 27454557L));
        items.add(new Item("univ_batch_13.nt.gz", 27429181L));
        items.add(new Item("univ_batch_14.nt.gz", 27465464L));
        items.add(new Item("univ_batch_15.nt.gz", 26983475L));
        items.add(new Item("univ_batch_16.nt.gz", 26763955L));
        items.add(new Item("univ_batch_17.nt.gz", 27222346L));
        items.add(new Item("univ_batch_18.nt.gz", 27049406L));
        items.add(new Item("univ_batch_19.nt.gz", 27742169L));
        items.add(new Item("univ_batch_21.nt.gz", 26789890L));
        items.add(new Item("univ_batch_22.nt.gz", 26810562L));
        items.add(new Item("univ_batch_23.nt.gz", 27021905L));
        items.add(new Item("univ_batch_27.nt.gz", 26995232L));
        items.add(new Item("univ_batch_28.nt.gz", 27311305L));
        items.add(new Item("univ_batch_29.nt.gz", 27440298L));
        items.add(new Item("univ_batch_30.nt.gz", 27216167L));
        items.add(new Item("univ_batch_31.nt.gz", 27139464L));
        items.add(new Item("univ_batch_32.nt.gz", 27371138L));
        items.add(new Item("univ_batch_33.nt.gz", 26720305L));
        items.add(new Item("univ_batch_34.nt.gz", 27014352L));
        items.add(new Item("univ_batch_35.nt.gz", 26916725L));
        items.add(new Item("univ_batch_36.nt.gz", 26774798L));
        items.add(new Item("univ_batch_37.nt.gz", 27861577L));
        items.add(new Item("univ_batch_38.nt.gz", 27503074L));
        items.add(new Item("univ_batch_39.nt.gz", 27284765L));
        items.add(new Item("univ_batch_40.nt.gz", 27252030L));
        items.add(new Item("univ_batch_41.nt.gz", 27141616L));
        items.add(new Item("univ_batch_42.nt.gz", 27475049L));
        items.add(new Item("univ_batch_43.nt.gz", 26929211L));
        items.add(new Item("univ_batch_44.nt.gz", 27400437L));
        items.add(new Item("univ_batch_45.nt.gz", 26924724L));
        items.add(new Item("univ_batch_46.nt.gz", 27273479L));
        items.add(new Item("univ_batch_47.nt.gz", 27120736L));
        items.add(new Item("univ_batch_48.nt.gz", 26865918L));
        items.add(new Item("univ_batch_49.nt.gz", 27544895L));
        items.add(new Item("univ_batch_50.nt.gz", 28692331L));
        items.add(new Item("univ_batch_51.nt.gz", 27944226L));
        items.add(new Item("univ_batch_52.nt.gz", 27182050L));
        items.add(new Item("univ_batch_53.nt.gz", 27583966L));
        items.add(new Item("univ_batch_54.nt.gz", 26537008L));
        items.add(new Item("univ_batch_55.nt.gz", 27429874L));
        items.add(new Item("univ_batch_56.nt.gz", 27053563L));
        items.add(new Item("univ_batch_57.nt.gz", 26758372L));
        items.add(new Item("univ_batch_58.nt.gz", 26949008L));
        items.add(new Item("univ_batch_59.nt.gz", 27271196L));
        items.add(new Item("univ_batch_60.nt.gz", 27042743L));
        items.add(new Item("univ_batch_61.nt.gz", 27094369L));
        items.add(new Item("univ_batch_62.nt.gz", 27268390L));
        items.add(new Item("univ_batch_63.nt.gz", 27411310L));
        items.add(new Item("univ_batch_64.nt.gz", 27395500L));
        items.add(new Item("univ_batch_65.nt.gz", 26994741L));
        items.add(new Item("univ_batch_66.nt.gz", 27284488L));
        items.add(new Item("univ_batch_67.nt.gz", 27401758L));
        items.add(new Item("univ_batch_68.nt.gz", 26933068L));
        items.add(new Item("univ_batch_69.nt.gz", 27156929L));
        items.add(new Item("univ_batch_70.nt.gz", 27684024L));
        items.add(new Item("univ_batch_71.nt.gz", 27389288L));
        items.add(new Item("univ_batch_72.nt.gz", 26810624L));
        items.add(new Item("univ_batch_73.nt.gz", 27020423L));
        items.add(new Item("univ_batch_74.nt.gz", 27721304L));
        items.add(new Item("univ_batch_75.nt.gz", 27898120L));
        items.add(new Item("univ_batch_76.nt.gz", 26739789L));
        items.add(new Item("univ_batch_77.nt.gz", 27986071L));
        items.add(new Item("univ_batch_78.nt.gz", 27339646L));
        items.add(new Item("univ_batch_79.nt.gz", 26947773L));
        items.add(new Item("univ_batch_80.nt.gz", 26885430L));
        items.add(new Item("univ_batch_81.nt.gz", 26608008L));
        items.add(new Item("univ_batch_82.nt.gz", 27896405L));
        items.add(new Item("univ_batch_83.nt.gz", 26814208L));
        items.add(new Item("univ_batch_84.nt.gz", 27477764L));
        items.add(new Item("univ_batch_85.nt.gz", 26748511L));
        items.add(new Item("univ_batch_86.nt.gz", 26783360L));
        items.add(new Item("univ_batch_87.nt.gz", 27051593L));
        items.add(new Item("univ_batch_88.nt.gz", 27754659L));
        items.add(new Item("univ_batch_89.nt.gz", 27670319L));
        items.add(new Item("univ_batch_90.nt.gz", 26579985L));
        items.add(new Item("univ_batch_91.nt.gz", 27542415L));
        items.add(new Item("univ_batch_92.nt.gz", 27283291L));
        items.add(new Item("univ_batch_93.nt.gz", 28104960L));
        items.add(new Item("univ_batch_94.nt.gz", 26449756L));
        items.add(new Item("univ_batch_95.nt.gz", 27639619L));
        items.add(new Item("univ_batch_96.nt.gz", 27472429L));
        items.add(new Item("univ_batch_97.nt.gz", 27025758L));
        items.add(new Item("univ_batch_98.nt.gz", 26762203L));
        items.add(new Item("univ_batch_99.nt.gz", 26569525L));
        items.add(new Item("univ_batch_25.nt.gz", 27254990L));
        items.add(new Item("univ_batch_24.nt.gz", 27633131L));
        items.add(new Item("univ_batch_26.nt.gz", 27554346L));
        items.add(new Item("univ_batch_100.nt.gz",27498180L));
        items.add(new Item("univ_batch_101.nt.gz",27447008L));
        items.add(new Item("univ_batch_102.nt.gz",27765454L));
        items.add(new Item("univ_batch_103.nt.gz",27169824L));
        items.add(new Item("univ_batch_104.nt.gz",27237442L));
        items.add(new Item("univ_batch_105.nt.gz",27110522L));
        items.add(new Item("univ_batch_106.nt.gz",27360708L));
        items.add(new Item("univ_batch_107.nt.gz",27184554L));
        items.add(new Item("univ_batch_108.nt.gz",26713684L));
        items.add(new Item("univ_batch_109.nt.gz",27215059L));
        items.add(new Item("univ_batch_110.nt.gz",27274947L));
        items.add(new Item("univ_batch_111.nt.gz",26842104L));
        items.add(new Item("univ_batch_112.nt.gz",26828452L));
        items.add(new Item("univ_batch_113.nt.gz",27413656L));
        items.add(new Item("univ_batch_114.nt.gz",27875783L));
        items.add(new Item("univ_batch_115.nt.gz",26782630L));
        items.add(new Item("univ_batch_116.nt.gz",27693285L));
        items.add(new Item("univ_batch_117.nt.gz",26928806L));
        items.add(new Item("univ_batch_118.nt.gz",27708433L));
        items.add(new Item("univ_batch_119.nt.gz",26834711L));
        items.add(new Item("univ_batch_120.nt.gz",27396865L));
        items.add(new Item("univ_batch_121.nt.gz",26824861L));
        items.add(new Item("univ_batch_122.nt.gz",26897316L));
        items.add(new Item("univ_batch_123.nt.gz",27694519L));
        items.add(new Item("univ_batch_124.nt.gz",27224584L));
        items.add(new Item("univ_batch_125.nt.gz",27586088L));
        items.add(new Item("univ_batch_126.nt.gz",27204205L));
        items.add(new Item("univ_batch_127.nt.gz",27557407L));
        items.add(new Item("univ_batch_128.nt.gz",27756072L));
        items.add(new Item("univ_batch_129.nt.gz",27466363L));
        items.add(new Item("univ_batch_130.nt.gz",27050640L));
        items.add(new Item("univ_batch_131.nt.gz",26268523L));
        items.add(new Item("univ_batch_132.nt.gz",28067144L));
        items.add(new Item("univ_batch_133.nt.gz",26366760L));
        items.add(new Item("univ_batch_134.nt.gz",18337116L));
        items.add(new Item("univ_batch_135.nt.gz",27286336L));
        items.add(new Item("univ_batch_136.nt.gz",27285709L));
        items.add(new Item("univ_batch_137.nt.gz",27859459L));
        items.add(new Item("univ_batch_138.nt.gz",27274762L));
        items.add(new Item("univ_batch_139.nt.gz",26911588L));
        items.add(new Item("univ_batch_140.nt.gz",27239832L));
        items.add(new Item("univ_batch_141.nt.gz",26915030L));
        items.add(new Item("univ_batch_142.nt.gz",26933749L));
        items.add(new Item("univ_batch_143.nt.gz",27093681L));
        items.add(new Item("univ_batch_144.nt.gz",27290176L));
        items.add(new Item("univ_batch_145.nt.gz",27432541L));
        items.add(new Item("univ_batch_146.nt.gz",26876418L));
        items.add(new Item("univ_batch_147.nt.gz",27392228L));
        items.add(new Item("univ_batch_148.nt.gz",27502666L));
        items.add(new Item("univ_batch_149.nt.gz",27543932L));
        items.add(new Item("univ_batch_150.nt.gz",27120292L));
        items.add(new Item("univ_batch_151.nt.gz",27086970L));
        items.add(new Item("univ_batch_152.nt.gz",27222235L));
        items.add(new Item("univ_batch_153.nt.gz",27234810L));
        items.add(new Item("univ_batch_154.nt.gz",26585217L));
        items.add(new Item("univ_batch_155.nt.gz",27632191L));
        items.add(new Item("univ_batch_156.nt.gz",27094208L));
        items.add(new Item("univ_batch_157.nt.gz",27322681L));
        items.add(new Item("univ_batch_158.nt.gz",26565756L));
        items.add(new Item("univ_batch_159.nt.gz",27811160L));
        items.add(new Item("univ_batch_160.nt.gz",27245676L));
        items.add(new Item("univ_batch_161.nt.gz",27059130L));
        items.add(new Item("univ_batch_162.nt.gz",27489756L));
        items.add(new Item("univ_batch_163.nt.gz",26714657L));
        items.add(new Item("univ_batch_164.nt.gz",27202436L));
        items.add(new Item("univ_batch_165.nt.gz",27471692L));
        items.add(new Item("univ_batch_166.nt.gz",26946292L));
        items.add(new Item("univ_batch_167.nt.gz",27208985L));
        items.add(new Item("univ_batch_168.nt.gz",26959231L));
        items.add(new Item("univ_batch_169.nt.gz",26958601L));
        items.add(new Item("univ_batch_170.nt.gz",27611921L));
        items.add(new Item("univ_batch_171.nt.gz",27842561L));
        items.add(new Item("univ_batch_172.nt.gz",26781430L));
        items.add(new Item("univ_batch_173.nt.gz",27058222L));
        items.add(new Item("univ_batch_174.nt.gz",27106182L));
        items.add(new Item("univ_batch_175.nt.gz",27407448L));
        items.add(new Item("univ_batch_176.nt.gz",27169044L));
        items.add(new Item("univ_batch_177.nt.gz",27653426L));
        items.add(new Item("univ_batch_178.nt.gz",27157210L));
        items.add(new Item("univ_batch_179.nt.gz",27013884L));
        items.add(new Item("univ_batch_180.nt.gz",27546914L));
        items.add(new Item("univ_batch_181.nt.gz",27165998L));
        items.add(new Item("univ_batch_182.nt.gz",26505965L));
        items.add(new Item("univ_batch_183.nt.gz",26703957L));
        items.add(new Item("univ_batch_184.nt.gz",27991730L));
        items.add(new Item("univ_batch_185.nt.gz",27576543L));
        items.add(new Item("univ_batch_186.nt.gz",27191118L));
        items.add(new Item("univ_batch_187.nt.gz",26592188L));
        items.add(new Item("univ_batch_188.nt.gz",27884637L));
        items.add(new Item("univ_batch_189.nt.gz",27702043L));
        items.add(new Item("univ_batch_190.nt.gz",27496790L));
        items.add(new Item("univ_batch_191.nt.gz",27525728L));
        items.add(new Item("univ_batch_192.nt.gz",26772818L));
        items.add(new Item("univ_batch_193.nt.gz",27691785L));
        items.add(new Item("univ_batch_194.nt.gz",27523072L));
        items.add(new Item("univ_batch_195.nt.gz",26433042L));
        items.add(new Item("univ_batch_196.nt.gz",27091115L));
        items.add(new Item("univ_batch_197.nt.gz",26958024L));
        items.add(new Item("univ_batch_198.nt.gz",27041472L));
        items.add(new Item("univ_batch_199.nt.gz",27288827L));
        items.add(new Item("univ_batch_200.nt.gz",26582934L));
        items.add(new Item("univ_batch_201.nt.gz",27575286L));
        items.add(new Item("univ_batch_202.nt.gz",27727407L));
        items.add(new Item("univ_batch_203.nt.gz",27136650L));
        items.add(new Item("univ_batch_204.nt.gz",26856022L));
        items.add(new Item("univ_batch_205.nt.gz",27141923L));
        items.add(new Item("univ_batch_206.nt.gz",27160839L));
        items.add(new Item("univ_batch_207.nt.gz",27061754L));
        items.add(new Item("univ_batch_208.nt.gz",27064034L));
        items.add(new Item("univ_batch_209.nt.gz",27317234L));
        items.add(new Item("univ_batch_210.nt.gz",27114293L));
        items.add(new Item("univ_batch_211.nt.gz",27410961L));
        items.add(new Item("univ_batch_212.nt.gz",26959398L));
        items.add(new Item("univ_batch_213.nt.gz",27037044L));
        items.add(new Item("univ_batch_214.nt.gz",28133555L));
        items.add(new Item("univ_batch_215.nt.gz",26909270L));
        items.add(new Item("univ_batch_216.nt.gz",26573673L));
        items.add(new Item("univ_batch_217.nt.gz",27221849L));
        items.add(new Item("univ_batch_218.nt.gz",27479958L));
        items.add(new Item("univ_batch_219.nt.gz",26497831L));
        items.add(new Item("univ_batch_220.nt.gz",27170446L));
        items.add(new Item("univ_batch_221.nt.gz",27908759L));
        items.add(new Item("univ_batch_222.nt.gz",27578793L));
        items.add(new Item("univ_batch_223.nt.gz",27135852L));
        items.add(new Item("univ_batch_224.nt.gz",26703123L));
        items.add(new Item("univ_batch_225.nt.gz",26998713L));
        items.add(new Item("univ_batch_226.nt.gz",27485770L));
        items.add(new Item("univ_batch_227.nt.gz",27709113L));
        items.add(new Item("univ_batch_228.nt.gz",27193256L));
        items.add(new Item("univ_batch_229.nt.gz",27336846L));
        items.add(new Item("univ_batch_230.nt.gz",26893407L));
        items.add(new Item("univ_batch_231.nt.gz",27835591L));
        items.add(new Item("univ_batch_232.nt.gz",27253524L));
        items.add(new Item("univ_batch_233.nt.gz",26968463L));
        items.add(new Item("univ_batch_234.nt.gz",26919484L));
        items.add(new Item("univ_batch_235.nt.gz",27495676L));
        items.add(new Item("univ_batch_236.nt.gz",27781525L));
        items.add(new Item("univ_batch_237.nt.gz",26891018L));
        items.add(new Item("univ_batch_238.nt.gz",27049768L));
        items.add(new Item("univ_batch_239.nt.gz",26907691L));
        items.add(new Item("univ_batch_240.nt.gz",27940826L));
        items.add(new Item("univ_batch_241.nt.gz",27730879L));
        items.add(new Item("univ_batch_242.nt.gz",27431119L));
        items.add(new Item("univ_batch_243.nt.gz",26945397L));
        items.add(new Item("univ_batch_244.nt.gz",26520638L));
        items.add(new Item("univ_batch_245.nt.gz",27546383L));
        items.add(new Item("univ_batch_246.nt.gz",27196090L));
        items.add(new Item("univ_batch_247.nt.gz",26600069L));
        items.add(new Item("univ_batch_248.nt.gz",27022528L));
        items.add(new Item("univ_batch_249.nt.gz",27500052L));
        items.add(new Item("univ_batch_250.nt.gz",27457992L));
        items.add(new Item("univ_batch_251.nt.gz",27176046L));
        items.add(new Item("univ_batch_252.nt.gz",27434147L));
        items.add(new Item("univ_batch_253.nt.gz",27054517L));
        items.add(new Item("univ_batch_254.nt.gz",27661865L));
        items.add(new Item("univ_batch_255.nt.gz",26829985L));
        items.add(new Item("univ_batch_256.nt.gz",27440976L));
        items.add(new Item("univ_batch_257.nt.gz",26879324L));
        items.add(new Item("univ_batch_258.nt.gz",26666231L));
        items.add(new Item("univ_batch_259.nt.gz",28140498L));
        items.add(new Item("univ_batch_260.nt.gz",27433651L));
        items.add(new Item("univ_batch_261.nt.gz",27077180L));
        items.add(new Item("univ_batch_262.nt.gz",27445535L));
        items.add(new Item("univ_batch_263.nt.gz",27771320L));
        items.add(new Item("univ_batch_264.nt.gz",27600320L));
        items.add(new Item("univ_batch_265.nt.gz",26831027L));
        items.add(new Item("univ_batch_266.nt.gz",27294949L));


        
        return items;
    }

}
