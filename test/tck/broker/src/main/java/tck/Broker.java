package tck;

import com.hivemq.embedded.EmbeddedHiveMQ;
import java.nio.file.Path;

/** Starts HiveMQ CE: Broker <config folder> <data folder> <extensions folder>. */
public class Broker {
    public static void main(String[] args) throws Exception {
        EmbeddedHiveMQ hivemq = EmbeddedHiveMQ.builder()
                .withConfigurationFolder(Path.of(args[0]))
                .withDataFolder(Path.of(args[1]))
                .withExtensionsFolder(Path.of(args[2]))
                .build();
        hivemq.start().join();
        System.out.println("BROKER READY");
        Thread.currentThread().join();
    }
}
