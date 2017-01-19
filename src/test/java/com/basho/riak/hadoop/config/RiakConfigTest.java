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
package com.basho.riak.hadoop.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.basho.riak.hadoop.keylisters.BucketKeyLister;
import com.basho.riak.hadoop.keylisters.KeyLister;
import com.basho.riak.hadoop.keylisters.KeysKeyLister;

/**
 * @author russell
 * 
 */
public class RiakConfigTest {

    private static final String BUCKET = "bucket";

    @Test public void setAndGetKeyLister() throws Exception {
        Configuration conf = new Configuration();

        BucketKeyLister bkl = new BucketKeyLister(BUCKET);
        conf = RiakConfig.setKeyLister(conf, bkl);
        KeyLister actual = RiakConfig.getKeyLister(conf);
        assertEquals(bkl, actual);

        KeysKeyLister kkl = new KeysKeyLister(Arrays.asList("k1", "k2", "k3", "k4"), BUCKET);
        conf = RiakConfig.setKeyLister(conf, kkl);
        actual = RiakConfig.getKeyLister(conf);
        assertEquals(kkl, actual);
    }
}
