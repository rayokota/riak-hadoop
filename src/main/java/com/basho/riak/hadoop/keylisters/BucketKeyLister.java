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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.RiakException;
import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.hadoop.BucketKey;

/**
 * A full list buckets key lister. DANGER, not advised for production use.
 * 
 * @author russell
 * 
 */
public class BucketKeyLister implements KeyLister {

    private static final String EMPTY = "";
    private String bucket;

    /**
     * no arg CTOR for de-serialization
     */
    public BucketKeyLister() {}

    /**
     * @param bucket
     */
    public BucketKeyLister(String bucket) {
        this.bucket = bucket;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.KeyLister#getKeys()
     */
    public Collection<BucketKey> getKeys(RiakClient client) throws RiakException {
        if (bucket == null || bucket.trim().equals(EMPTY)) {
            throw new IllegalStateException("bucket cannot be null or empty");
        }

        try {
            Namespace ns = new Namespace(bucket);
            ListKeys lk = new ListKeys.Builder(ns).build();
            ListKeys.Response response = client.execute(lk);
            List<BucketKey> keys = new ArrayList<BucketKey>();
            for (Location l : response) {
                String key = l.getKeyAsString();
                keys.add(new BucketKey(bucket, key));
            }
            return keys;
        } catch (Exception e) {
            throw new RiakException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.KeyLister#init(java.lang.String)
     */
    public void init(String bucket) {
        this.bucket = bucket;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.KeyLister#getInitString()
     */
    public String getInitString() {
        return bucket;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bucket == null) ? 0 : bucket.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof BucketKeyLister)) {
            return false;
        }
        BucketKeyLister other = (BucketKeyLister) obj;
        if (bucket == null) {
            if (other.bucket != null) {
                return false;
            }
        } else if (!bucket.equals(other.bucket)) {
            return false;
        }
        return true;
    }
}
