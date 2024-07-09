package com.scanntech.diticketsretropersonal.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class AvroSerializer<T extends SpecificRecord> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Configuración, si es necesario
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }

        SpecificDatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        try {
            datumWriter.write(data, binaryEncoder);
            binaryEncoder.flush();
            outputStream.close();
        } catch (IOException e) {
            throw new SerializationException("Error al serializar el objeto Avro", e);
        }

        return outputStream.toByteArray();
    }

    @Override
    public void close() {
        // Liberar recursos, si es necesario
    }
}
