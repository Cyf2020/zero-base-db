package cn.hk.orange.transport;

public class Packager {
    private final Transporter transpoter;
    private final Encoder encoder;

    public Packager(Transporter transpoter, Encoder encoder) {
        this.transpoter = transpoter;
        this.encoder = encoder;
    }

    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transpoter.send(data);
    }

    public Package receive() throws Exception {
        byte[] data = transpoter.receive();
        return encoder.decode(data);
    }

    public void close() throws Exception {
        transpoter.close();
    }
}
