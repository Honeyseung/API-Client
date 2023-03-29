import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class Client {
  private static ByteBuf createHeader(int identifier, short sender, short length) {
    ByteBuf byteBuf = Unpooled.buffer(0);
    byteBuf.writeInt(identifier);
    byteBuf.writeShort(sender);
    byteBuf.writeShort(length);
    byteBuf.writeInt((int) Instant.now().getEpochSecond());
    byteBuf.writeInt(Instant.now().getNano());
    return byteBuf;
  }

  private static Set<Class<?>> getClasses() {
    Set<Class<?>> classes = new HashSet<>();

    String directoryString = System.getProperty("user.dir") + "/src/main/java/messages";
    File directory = new File(directoryString);

    if (directory.exists()) {
      String[] files = directory.list();
      assert files != null;
      for (String fileName : files) {
        if (fileName.endsWith(".java")) {
          fileName = fileName.substring(0, fileName.length() - 5); // 클래스 이름만 추출
          try {
            Class<?> c = Class.forName("messages." + fileName);
            if (!Modifier.isAbstract(c.getModifiers())) // 추상 클래스 제외
            classes.add(c);
          } catch (ClassNotFoundException e) {
            System.err.println(fileName + " 클래스가 존재하지 않습니다.");
            e.printStackTrace();
          }
        }
      }
    } else {
      System.err.println("messages 파일이 존재하지 않습니다.");
    }
    return classes;
  }

  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] =
          (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  public static void main(String[] args)
      throws IOException, InstantiationException, IllegalAccessException {
    final Logger logger = LoggerFactory.getLogger(Client.class);

    Config config = ConfigFactory.load();
    String configPath = System.getProperty("config.file");

    if (configPath != null) {
      File f = new File(configPath);
      if (f.exists() && !f.isDirectory()) {
        config = ConfigFactory.parseFile(f).resolve();
        logger.info("Configuration is loaded. [{}]", f);
      } else {
        logger.error("Failed to load configuration. Please check the [-Dconfig.file] option.");
        System.exit(0);
      }
    } else {
      logger.debug("Configuration is loaded. (Development Mode)");
    }

    InetAddress address = InetAddress.getByName(config.getString("host"));
    List<Integer> portList = config.getIntList("portList");
    int interver = config.getInt("interver");
    //Set<Class<?>> classes = getClasses();
    Thread[] multiThreads = new Thread[portList.size()];
    MulticastSocket[] sockets = new MulticastSocket[portList.size()];
    for (int i = 0; i < portList.size(); i++) {
      //MessageData messageData = null;
      int port = portList.get(i);
      int fnum = i;
      //System.out.println(port + "포트 설정 중....");
      sockets[fnum] = new MulticastSocket(port);
      try {
        sockets[fnum].joinGroup(address);
      } catch (Exception e) {
        e.printStackTrace();
      }
      // for(Class<?> s:classes){
      //  if(s.getName().equals("messages.Port"+port)){
      //    messageData = MessageData.getData(s);
      //  }
      // }
      // if (messageData == null) { // 사용하지 않는 메세지는 패스
      //  System.out.println(portList.get(fnum) + "포트 사용하지 않음");
      //  continue;
      // } else
      System.out.println(portList.get(fnum) + "port joined");
      String filename =
              config.getString("ecsroot") + (port - 50000) + "_ecs.csv";
      File file = new File(filename);
      BufferedReader readfile = new BufferedReader(new FileReader(file));
      // ByteBuf byteBuf = createHeader(messageData.getCode(), (short) 1, (short)
      // (messageData.getBytes().readableBytes()+16));
      // byteBuf.writeBytes(messageData.getBytes());
      // byte[] bytes = new byte[byteBuf.readableBytes()];
      // byteBuf.getBytes(byteBuf.readerIndex(), bytes);
      multiThreads[i] =
          new Thread(
              () -> {
                String ecsmsg = null;
                while (true) {
                  try {
                    ecsmsg = readfile.readLine();
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                  assert ecsmsg != null;
                  String ecsbyte = ecsmsg.split("\\|")[2];
                  ecsbyte = ecsbyte.replace("0x", "");
                  byte[] bytes = hexStringToByteArray(ecsbyte);
                  DatagramPacket msg = new DatagramPacket(bytes, bytes.length, address, port);
                  System.out.println(
                      port
                          + "port Send: "
                          + DatatypeConverter.printHexBinary(msg.getData())
                          + " \nMessage_Identifier: "
                          + ecsmsg.split("\\|")[0]
                          + "\nMessage_Length: "
                          + msg.getLength()
                          + "\n");
                  try {
                    sockets[fnum].send(msg);
                    Thread.sleep(interver);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });
      multiThreads[i].start();
    }
  }
}
