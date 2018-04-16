package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.streaming.rdd.MapWithStateRDDRecord;

public class SparkKryoHTMRegistrator implements KryoRegistrator {

    public SparkKryoHTMRegistrator() {
    }

    @Override
    public void registerClasses(Kryo kryo) {
        // the rest HTM.java internal classes support Persistence API (with preSerialize/postDeserialize methods),
        // therefore we'll create the seralizers which will use HTMObjectInput/HTMObjectOutput (wrappers on top of fast-serialization)
        // which WILL call the preSerialize/postDeserialize
        SparkKryoHTMSerializer.registerSerializers(kryo);

        kryo.register(MonitoringRecord.class, new SparkKryoHTMSerializer());
        kryo.register(MapWithStateRDDRecord.class, new SparkKryoHTMSerializer());
    }
}