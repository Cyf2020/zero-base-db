package lock;

import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReentrantReadWriteLockTest {
    @Test
    public void read() {
        class Resource {
            private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            private final HashMap<String, String> hashMap = new HashMap<>();

            public String get(String key) {
                String res;
                lock.readLock().lock();
                try {
                    res = hashMap.get(key);
                } finally {
                    lock.readLock().unlock();
                }
                return res;
            }

            public void put(String key, String val) {
                lock.writeLock().lock();
                try {
                    hashMap.put(key, val);
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }

    }
}
