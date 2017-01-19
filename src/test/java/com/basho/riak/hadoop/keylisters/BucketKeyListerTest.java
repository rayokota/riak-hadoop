/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.hadoop.keylisters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.RiakException;
import com.basho.riak.hadoop.BucketKey;

/**
 * @author russell
 * 
 */
public class BucketKeyListerTest {

    private static final String BUCKET_NAME = "bucket";

    @Mock private RiakClient riakClient;
    @Mock private ListKeys.Response listKeysResponse;

    private BucketKeyLister lister;

    /**
     * Create {@link BucketKeyLister}, mocks, wire together, stub mocks
     */
    @Before public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        // stub default calls to IRiakClient and FetchBucket
        Namespace ns = new Namespace(BUCKET_NAME);
        ListKeys lk = new ListKeys.Builder(ns).build();
        when(riakClient.execute(lk)).thenReturn(listKeysResponse);
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.BucketKeyLister#BucketKeyLister()}.
     */
    @Test public void illegalState() throws Exception {
        lister = new BucketKeyLister();
        try {
            testLister(lister);
            fail("expected IllegalStateException");
        } catch (IllegalStateException e) {
            // NO-OP
        }
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.BucketKeyLister#BucketKeyLister(java.lang.String)}
     * .
     */
    @Test public void createWithBucket() throws Exception {
        lister = new BucketKeyLister(BUCKET_NAME);
        testLister(lister);
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.BucketKeyLister#init(java.lang.String)}.
     */
    @Test public void initWithBucket() throws Exception {
        lister = new BucketKeyLister();
        lister.init(BUCKET_NAME);
        testLister(lister);
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.BucketKeyLister#getInitString()}.
     */
    @Test public void testGetInitString() throws Exception {
        String initString = new BucketKeyLister(BUCKET_NAME).getInitString();
        assertEquals(BUCKET_NAME, initString);
        testLister(new BucketKeyLister(initString));
    }

    @Test public void zeroKeys() throws Exception {
        lister = new BucketKeyLister(BUCKET_NAME);
        testLister(lister, new ArrayList<String>());
    }

    private void testLister(BucketKeyLister lister) throws Exception {
        testLister(lister, Arrays.asList("k1", "k2", "k3", "k4"));
    }

    private void testLister(BucketKeyLister lister, List<String> expectedKeys) throws Exception {
        Namespace ns = new Namespace(BUCKET_NAME);
        when(listKeysResponse.iterator()).thenReturn(
                expectedKeys.stream().map(s -> new Location(ns, s)).collect(Collectors.toList()).iterator());
        Collection<BucketKey> keys = lister.getKeys(riakClient);
        assertEquals("Expected keys to be same length as stubbed mock value", expectedKeys.size(), keys.size());

        for (String k : expectedKeys) {
            assertTrue("Expected keys to contain " + k, keys.contains(new BucketKey(BUCKET_NAME, k)));
        }
    }
}
