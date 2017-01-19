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
import java.util.ArrayList;
import java.util.Collection;

import com.basho.riak.client.api.cap.ConflictResolverFactory;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import org.apache.hadoop.mapreduce.Mapper;

import com.basho.riak.client.api.cap.ConflictResolver;
import com.basho.riak.client.api.convert.Converter;
import com.basho.riak.client.core.query.RiakObject;

/**
 * A Riak specific extension of {@link Mapper} that can be used if you wish to
 * work with domain specific types and handle sibling values in your
 * {@link Mapper#map} method
 * 
 * @author russell
 * @param <T>
 *            the type for the input value
 * @param <OK>
 *            the type for the out key
 * @param <OV>
 *            the type for the out value
 * 
 */
public abstract class RiakMapper<T, OK, OV> extends Mapper<BucketKey, RiakObject, OK, OV> {

    private final Class<T> cls;
    private final Converter<T> converter;
    private final ConflictResolver<T> resolver;

    /**
     * Create a {@link Mapper} that will use the provided {@link Converter} and
     * {@link ConflictResolver} on the raw {@link RiakObject} returned by the
     * {@link RiakRecordReader}
     * 
     * @param converter
     *            a {@link Converter}
     * @param resolver
     *            a {@link ConflictResolver}
     */
    public RiakMapper(Class<T> cls, Converter<T> converter, ConflictResolver<T> resolver) {
        this.cls = cls;
        this.converter = converter;
        this.resolver = resolver;

        ConflictResolverFactory factory = ConflictResolverFactory.getInstance();
        factory.registerConflictResolver(cls, resolver);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override public void map(BucketKey key, RiakObject value, Context context) throws IOException,
            InterruptedException {

        Namespace bucket = new Namespace(key.getBucket());
        Location location = new Location(bucket, key.getKey());
        map(key, converter.toDomain(value, location), context);

        /*
        // convert, conflict resolve
        final Collection<T> siblings = new ArrayList<T>(value.numberOfValues());

        for (RiakObject o : value) {
            siblings.add(converter.toDomain(o));
        }

        map(key, resolver.resolve(siblings), context);
        */
    }

    /**
     * Override this method in your {@link Mapper}. It is called by the default
     * {@link Mapper#map} method, after applying the {@link Converter} and
     * {@link ConflictResolver}. Put your mapping code here.
     * 
     * @param k
     *            the {@link BucketKey}
     * @param value
     *            the converted value
     * @param context
     *            the hadoop job Context
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void map(BucketKey k, T value, Context context) throws IOException, InterruptedException;

}
