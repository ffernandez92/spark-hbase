package com.spark.resources.flume;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

/**
 * Decodes a serialized Flume Event stored in Kafka.
 * @author ffernandez92
 *
 */
public class FlumeToKafkaDecoder implements Decoder<Event> {
    
    private static final Logger logger = LoggerFactory.getLogger(FlumeToKafkaDecoder.class);

    private final DatumReader<AvroFlumeEvent> datumReader = new SpecificDatumReader<>(AvroFlumeEvent.class);

    private BinaryDecoder decoder;

    public FlumeToKafkaDecoder(final VerifiableProperties verifiableProperties) {

    }

    /**
     * Decode a serialized flume event.
     * 
     * @param bytes is a serialized flume event.
     * @return a decoded flume event.
     */
     @Override
     public Event fromBytes(byte[] bytes) {
	try {
		final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
		decoder = DecoderFactory.get().directBinaryDecoder(inputStream, decoder);
		final AvroFlumeEvent avroFlumeEvent = datumReader.read(null, decoder);
		return EventBuilder.withBody(avroFlumeEvent.getBody().array(), toStringMap(avroFlumeEvent.getHeaders()));
	} catch (Exception exception) {
		logger.error("Cannot decode flume event [" + bytes + "]", exception);
		return null;
	}
     }

     /**
      * Convert a <CharSequence, CharSequence> map, to a <String, String> map.
      * 
      * @param charSequenceMap is the <CharSequence, CharSequence> map to convert.
      * @return a new <String, String> map containing all the values from charSequenceMap converted to String.
      */
     private Map<String, String> toStringMap(final Map<CharSequence, CharSequence> charSequenceMap) {
	if (null == charSequenceMap) {
		return null;
	}
	final Map<String, String> stringMap = new HashMap<>();
	for (Map.Entry<CharSequence, CharSequence> entry : charSequenceMap.entrySet()) {
		stringMap.put(entry.getKey().toString(), entry.getValue().toString());
		}
		return stringMap;
	}
}
