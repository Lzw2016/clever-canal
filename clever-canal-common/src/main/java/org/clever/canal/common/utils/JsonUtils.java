package org.clever.canal.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.*;

import java.lang.reflect.Type;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

/**
 * 字节处理相关工具类
 */
@SuppressWarnings("unused")
public class JsonUtils {

    static {
        SerializeConfig.getGlobalInstance().put(InetAddress.class, InetAddressSerializer.instance);
        SerializeConfig.getGlobalInstance().put(Inet4Address.class, InetAddressSerializer.instance);
        SerializeConfig.getGlobalInstance().put(Inet6Address.class, InetAddressSerializer.instance);
        // ParserConfig.getGlobalInstance().setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance().addAccept("com.alibaba.otter.");
        ParserConfig.getGlobalInstance().addAccept("com.taobao.tddl.dbsync.");
    }

    public static <T> T unmarshalFromByte(byte[] bytes, Class<T> targetClass) {
        return JSON.parseObject(bytes, targetClass);// 默认为UTF-8
    }

    public static <T> T unmarshalFromByte(byte[] bytes, TypeReference<T> type) {
        return JSON.parseObject(bytes, type.getType());
    }

    public static byte[] marshalToByte(Object obj) {
        return JSON.toJSONBytes(obj); // 默认为UTF-8
    }

    public static byte[] marshalToByte(Object obj, SerializerFeature... features) {
        return JSON.toJSONBytes(obj, features); // 默认为UTF-8
    }

    public static <T> T unmarshalFromString(String json, Class<T> targetClass) {
        return JSON.parseObject(json, targetClass);// 默认为UTF-8
    }

    public static <T> T unmarshalFromString(String json, TypeReference<T> type) {
        return JSON.parseObject(json, type);// 默认为UTF-8
    }

    public static String marshalToString(Object obj) {
        return JSON.toJSONString(obj); // 默认为UTF-8
    }

    public static String marshalToString(Object obj, SerializerFeature... features) {
        return JSON.toJSONString(obj, features); // 默认为UTF-8
    }

    /**
     * 可以允许指定一些过滤字段进行生成json对象
     */
    public static String marshalToString(Object obj, String... fliterFields) {
        final List<String> propertyFliters = Arrays.asList(fliterFields);
        try (SerializeWriter out = new SerializeWriter()) {
            JSONSerializer serializer = new JSONSerializer(out);
            serializer.getPropertyFilters().add((source, name, value) -> !propertyFliters.contains(name));
            serializer.write(obj);
            return out.toString();
        }
    }

    public static class InetAddressSerializer implements ObjectSerializer {

        public static InetAddressSerializer instance = new InetAddressSerializer();

        @Override
        public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) {
            if (object == null) {
                serializer.writeNull();
                return;
            }
            InetAddress address = (InetAddress) object;
            // 优先使用name
            serializer.write(address.getHostName());
        }
    }
}
