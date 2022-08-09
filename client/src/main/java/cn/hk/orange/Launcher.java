package cn.hk.orange;

import java.io.IOException;
import java.net.Socket;

import cn.hk.orange.transport.Encoder;
import cn.hk.orange.transport.Packager;
import cn.hk.orange.transport.Transporter;

public class Launcher {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
