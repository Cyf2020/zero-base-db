package cn.hk.orange.transport;

import java.util.Arrays;

import cn.hk.orange.exception.ServerError;
import com.google.common.primitives.Bytes;

public class Encoder {

    public byte[] encode(Package pkg) {
        // 判空
        if(pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if(err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            // SQL语句字节数组前加0
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    public Package decode(byte[] data) throws Exception {
        if(data.length < 1) {
            throw ServerError.InvalidPkgDataException;
        }
        if(data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if(data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw ServerError.InvalidPkgDataException;
        }
    }

}
