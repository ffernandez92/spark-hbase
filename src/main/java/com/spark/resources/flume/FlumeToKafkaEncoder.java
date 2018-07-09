package com.spark.resources.flume;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Event;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.common.serialization.Serializer;

/**
 * <p>It is used to serialize a Flume Event and store it into Kafka. 
 * It can be used at any Kafka client implementation adding the corresponding Kafka properties.</p>
 * 
 * <p>
 * <li><b>e.g.:</b></li>
 * Properties props = new Properties();<br>
 * props.put("zk.connect", “127.0.0.1:2181”);<br>
 * <b>props.put("value.serializer", "com.spark.resources.flume.FlumeToKafkaEncoder");</b><br>
 * </p>
 * 
 * @author ffernandez92
 *
 */
public class FlumeToKafkaEncoder implements Serializer<Event> {

	private ByteArrayOutputStream tempOutStream;
	private SpecificDatumWriter<AvroFlumeEvent> writer;
	private BinaryEncoder encoder = null;

	@Override
	public void close() {
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	
	}

	@Override
	public byte[] serialize(String topic, Event event) {
		if (event == null) {
			return null;
		} else {
			tempOutStream = new ByteArrayOutputStream();
			writer = new SpecificDatumWriter<>(AvroFlumeEvent.class);

			AvroFlumeEvent e = new AvroFlumeEvent(toCharSeqMap(event.getHeaders()), ByteBuffer.wrap(event.getBody()));
			encoder = EncoderFactory.get().directBinaryEncoder(tempOutStream, encoder);
			try {
				writer.write(e, encoder);
				encoder.flush();
				return tempOutStream.toByteArray();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			return null;
		}
	}

	private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
		Map<CharSequence, CharSequence> charSeqMap = new HashMap<>();
		for (Map.Entry<String, String> entry : stringMap.entrySet()) {
			charSeqMap.put(entry.getKey(), entry.getValue());
		}
		return charSeqMap;
	}

}
