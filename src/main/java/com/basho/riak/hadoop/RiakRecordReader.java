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
package com.basho.riak.hadoop;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.RiakException;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.BigIntIndex;
import com.basho.riak.hadoop.config.ClientFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Wrapper around a {@link RiakClient} for reading values from Riak
 * 
 * @author russell
 * 
 */
public class RiakRecordReader extends RecordReader<BucketKey, RiakObject> {

    private RiakClient client;
    private ConcurrentLinkedQueue<BucketKey> keys;
    private long initialSize;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override public void close() throws IOException {}

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override public BucketKey getCurrentKey() throws IOException, InterruptedException {
        return keys.peek();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
    @Override public RiakObject getCurrentValue() throws IOException, InterruptedException {
        try {
            BucketKey key = keys.poll();
            Namespace bucket = new Namespace(key.getBucket());
            Location location = new Location(bucket, key.getKey());
            FetchValue fetchOp = new FetchValue.Builder(location).build();
            return client.execute(fetchOp).getValue(RiakObject.class);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     */
    @Override public float getProgress() throws IOException, InterruptedException {
        int size = keys.size();
        if (size == 0) {
            return 0;
        } else {
            return size / initialSize;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop
     * .mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override public void initialize(InputSplit split, TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        try {
            RiakInputSplit inputSplit = (RiakInputSplit) split;
            keys = new ConcurrentLinkedQueue<BucketKey>(inputSplit.getInputs());
            initialSize = split.getLength();
            client = ClientFactory.getClient(inputSplit.getLocation());
        } catch (RiakException e) {
            throw new IOException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    @Override public boolean nextKeyValue() throws IOException, InterruptedException {
        return keys.peek() != null;
    }
}
