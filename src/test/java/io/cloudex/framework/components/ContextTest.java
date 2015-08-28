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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ContextTest {

    private Context context;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        Map<String, Object> data = Maps.newHashMap();
        data.put("bucket", "testing");
        data.put("greating", "hello");
        data.put("size", 50);

        context = new Context(data);

        data = Maps.newHashMap();

        context.putReadOnly("processors", Sets.newHashSet("m1234", "p0000", "k9888"));
    }


    /**
     * Test method for {@link io.cloudex.framework.components.Context#resolveKey(java.lang.String)}.
     */
    @Test
    public void testResolveKey() {
        String key = Context.resolveKey("#bucket");
        assertEquals("bucket", key);

    }

    /**
     * Test method for {@link io.cloudex.framework.components.Context#getKeyReference(java.lang.String)}.
     */
    @Test
    public void testGetKeyReference() {
        assertEquals("#bucket", Context.getKeyReference("bucket"));
    }

    /**
     * Test method for {@link io.cloudex.framework.components.Context#resolveValue(java.lang.String)}.
     */
    @Test
    public void testResolveValue() {
        Object value = context.resolveValue("bucket");
        assertNotNull(value);
        assertEquals("bucket", value);

        value = context.resolveValue(Context.getKeyReference("bucket"));

        assertNotNull(value);
        assertEquals("testing", value);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(expected = UnsupportedOperationException.class)
    public void testResolveReadOnlyValue() {
        
        Object value = context.resolveValue("#processors");
        System.out.println(value);
        assertNotNull(value);
        assertTrue(value instanceof Collection);
        
        Collection processors = (Collection) value;
        
        processors.add("garbage");
        
    }

    /**
     * Test method for {@link io.cloudex.framework.components.Context#resolveValues(java.util.Map)}.
     */
    @Test
    public void testResolveValues() {
        Map<String, String> keys = Maps.newHashMap();
        keys.put("mybucket", "#bucket");
        keys.put("mysize", "#size");
        keys.put("mygreating", "#greating");
        keys.put("blah", "somevalue");

        Map<String, Object> data = context.resolveValues(keys);

        assertNotNull(data);
        assertEquals(4, data.size());

        assertEquals("testing", data.get("mybucket"));
        assertEquals(50, data.get("mysize"));
        assertEquals("hello", data.get("mygreating"));
        assertEquals("somevalue", data.get("blah"));

        context.putReadOnly("size", 20);

        data = context.resolveValues(keys);

        assertNotNull(data);
        assertEquals(4, data.size());
        
        assertEquals("testing", data.get("mybucket"));
        // the read only value should take precedence
        assertEquals(20, data.get("mysize"));
        assertEquals("hello", data.get("mygreating"));
        assertEquals("somevalue", data.get("blah"));

    }

}
