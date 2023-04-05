import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.List;

public class Client {

  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] =
          (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  public static void main(String[] args) throws IOException {
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
    int interval = config.getInt("interval");

    Thread[] multiThreads = new Thread[portList.size()];
    MulticastSocket[] sockets = new MulticastSocket[portList.size()];

    for (int i = 0; i < portList.size(); i++) {
      int port = portList.get(i);
      int fnum = i;

      sockets[fnum] = new MulticastSocket(port);
      try {
        sockets[fnum].joinGroup(address);
      } catch (Exception e) {
        logger.error(String.valueOf(e));
      }

      logger.info(portList.get(fnum) + "port joined");
      String filename = config.getString("ecsroot") + "/" + (port - 50000) + "_ecs.csv";
      File file = new File(filename);
      BufferedReader readfile = new BufferedReader(new FileReader(file));

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

                  logger.info(
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
                    Thread.sleep(interval);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });
      multiThreads[i].start();
    }
  }
}
