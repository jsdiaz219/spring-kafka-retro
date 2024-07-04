package com.scanntech.diticketsretropersonal.avro;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    private final DatumReader<T> datumReader;

    public AvroDeserializer(Class<T> targetType) {
        this.datumReader = new SpecificDatumReader<>(targetType);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Configuraciones adicionales, si es necesario
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(bis, null);
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException("Error deserializing Avro message for topic " + topic, e);
        }
    }

    @Override
    public void close() {
        // Cerrar recursos, si es necesario
    }
}

