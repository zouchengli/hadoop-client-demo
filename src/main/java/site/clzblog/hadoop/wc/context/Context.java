package site.clzblog.hadoop.wc.context;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Context {
    private Map<Object, Object> map = new ConcurrentHashMap<Object, Object>();

    public Map<Object, Object> getMap() {
        return map;
    }

    public void write(Object key, Object value) {
        map.put(key, value);
    }

    public Object get(Object key) {
        return map.get(key);
    }
}
