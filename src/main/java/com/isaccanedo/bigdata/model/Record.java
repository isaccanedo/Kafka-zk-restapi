package com.isaccanedo.bigdata.model;

import java.nio.ByteBuffer;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.utils.Bytes;
import com.isaccanedo.bigdata.utils.KafkaUtils;

@Data
@Getter
@Setter
@Builder
@Log4j2
public class Record {
  public String topic;
  public long offset;
  public Object key;
  public Object value;
  public long timestamp;
  String keyDecoder;
  String valueDecoder;

  public String getValueByDecoder(String decoder, Object value) {
    if (value == null) return null;
    Class<?> type = KafkaUtils.DESERIALIZER_TYPE_MAP.get(decoder);
    try {
      if (String.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Short.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Integer.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Long.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Float.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Double.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Bytes.class.isAssignableFrom(type)) {
        Bytes bytes = (Bytes) value;
        return bytes.toString();
      }

      if (byte[].class.isAssignableFrom(type)) {
        if (decoder.contains("AvroDeserializer")) {
          return value.toString();
        } else {
          byte[] byteArray = (byte[]) value;
          return new String(byteArray);
        }
      }

      if (ByteBuffer.class.isAssignableFrom(type)) {
        ByteBuffer byteBuffer = (ByteBuffer) value;
        return new String(byteBuffer.array());
      }
    } catch (Exception exception) {
      throw new ApiException("Record Cast exception." + exception);
    }

    throw new ApiException(
        "Unknown class. Supported types are: "
            + "String, Short, Integer, Long, Float, Double, ByteArray, ByteBuffer, Bytes");
  }

  public String getValue() {
    log.info("getValue for value:" + value + " by decoder:" + valueDecoder);
    return getValueByDecoder(valueDecoder, value);
  }

  public String getKey() {
    log.info("getKeyValue for key:" + key + " by decoder:" + keyDecoder);
    return getValueByDecoder(keyDecoder, key);
  }

  @Override
  public String toString() {
    if (value != null) {
      return "topic:"
          + topic
          + ", offset:"
          + offset
          + ", key:"
          + getKey()
          + ", value:"
          + getValue()
          + ", timestamp:"
          + timestamp
          + ", keyDecoder:"
          + keyDecoder
          + ", valueDecoder:"
          + valueDecoder;
    } else {
      return "topic:"
          + topic
          + ", offset:"
          + offset
          + ", key:"
          + key
          + ", value:"
          + value
          + ", timestamp:"
          + timestamp
          + ", keyDecoder:"
          + keyDecoder
          + ", valueDecoder:"
          + valueDecoder;
    }
  }
}
