import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.lang.reflect.Field;

public class MessageData {
  private final int code;
  private final ByteBuf bytes;

  MessageData(int code, ByteBuf bytes) {
    this.code = code;
    this.bytes = bytes;
  }

  public int getCode() {
    return code;
  }

  public ByteBuf getBytes() {
    return bytes;
  }

  public static MessageData getData(Class<?> c)
      throws InstantiationException, IllegalAccessException {
    ByteBuf bytes = Unpooled.buffer(0);
    String[] str = c.getName().split("\\.");
    int code = 0;
    for (Field field : c.getDeclaredFields()) {
      field.setAccessible(true);
      if (field.getName().equals("use")) {
        if (!(boolean) field.get(c.newInstance())) return null;
        continue;
      }
      if (field.getName().equals("messageCode")) {
        code = (int) field.get(c.newInstance());
        continue;
      } else if (field.getType() == byte[].class)
        System.out.println(
            "FieldName: "
                + field.getName()
                + " | Type: "
                + field.getType()
                + " | Value: "
                + javax.xml.bind.DatatypeConverter.printHexBinary(
                    (byte[]) field.get(c.newInstance())));
      else
        System.out.println(
            "FieldName: "
                + field.getName()
                + " | Type: "
                + field.getType()
                + " | Value: "
                + field.get(c.newInstance()));

      if (field.getType() == byte.class) {
        bytes.writeByte((byte) field.get(c.newInstance()));
      } else if (field.getType() == Boolean.class) {
        bytes.writeBytes((byte[]) field.get(c.newInstance()));
      } else if (field.getType() == int.class) {
        bytes.writeInt((int) field.get(c.newInstance()));
      } else if (field.getType() == short.class) {
        bytes.writeShort((short) field.get(c.newInstance()));
      } else if (field.getType() == float.class) {
        bytes.writeFloat((float) field.get(c.newInstance()));
      } else if (field.getType() == double.class) {
        bytes.writeDouble((double) field.get(c.newInstance()));
      } else if (field.getType() == long.class) {
        bytes.writeLong((long) field.get(c.newInstance()));
      } else if (field.getType() == String.class) {
        bytes.writeBytes(((String) field.get(c.newInstance())).getBytes());
      } else System.out.println("Not Used Type");
    }
    return new MessageData(code, bytes);
  }
}
