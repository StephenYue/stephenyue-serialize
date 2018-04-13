package com.stephenyue.common.serialize;

import com.stephenyue.common.serialize.exception.DeserializeException;
import com.stephenyue.common.serialize.exception.SerializeException;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class ProtoBuffer {

    /**
     * 序列化普通对象
     * @param o
     * @param <T>
     * @return
     */
    public static <T> byte[] serializer(T o) {
        if (Objects.isNull(o)) {
            throw new SerializeException("被序列号的对象为null");
        }

        Schema schema = RuntimeSchema.getSchema(o.getClass());
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] bytes = null;
        try {
            bytes = ProtobufIOUtil.toByteArray(o, schema, buffer);
        } catch (Exception e) {
            throw new SerializeException("序列化(" + o.getClass() + ")对象(" + o + ")发生异常", e);
        } finally {
            buffer.clear();
        }

        return bytes;
    }

    /**
     * 反序列号普通对象
     * @param bytes
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T deserializer(byte[] bytes, Class<T> clazz) {
        if (Objects.isNull(bytes) || bytes.length == 0) {
            throw new DeserializeException("反序列化对象内容为空");
        }

        T obj = null;
        try {
            obj = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new DeserializeException("反序列化过程中依据类型创建对象失败", e);
        }

        if (Objects.nonNull(obj)) {
            Schema schema = RuntimeSchema.getSchema(obj.getClass());
            ProtostuffIOUtil.mergeFrom(bytes, obj, schema);
        }

        return obj;
    }

    /**
     * 序列号对象列表
     * @param l
     * @param <T>
     * @return
     */
    public static <T> byte[] serializer(List<T> l) {
        if (Objects.isNull(l) || l.isEmpty()) {
            throw new SerializeException("序列化对象列表为空");
        }

        Schema<T> schema = (Schema<T>) RuntimeSchema.getSchema(l.get(0).getClass());
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE * 1024);
        byte[] protostuff = null;
        ByteArrayOutputStream bos = null;
        try {
            bos = new ByteArrayOutputStream();
            ProtostuffIOUtil.writeListTo(bos, l, schema, buffer);
            protostuff = bos.toByteArray();
        } catch (Exception e) {
            throw new SerializeException("序列化对象列表(" + l + ")发生异常", e);
        } finally {
            buffer.clear();
            try {
                if (bos != null) {
                    bos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return protostuff;
    }

    /**
     * 用来反序列列表
     * @param bytes
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> List<T> deserializers(byte[] bytes, Class<T> clazz) {
        if (Objects.isNull(bytes) || bytes.length == 0) {
            throw new SerializeException("反序列化对象内容为空");
        }

        Schema<T> schema = RuntimeSchema.getSchema(clazz);
        List<T> result = null;
        try {
            result = ProtostuffIOUtil.parseListFrom(new ByteArrayInputStream(bytes), schema);
        } catch (IOException e) {
            throw new DeserializeException("反序列化对象列表发生异常!", e);
        }
        return result;
    }
}
