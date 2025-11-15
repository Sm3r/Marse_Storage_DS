package ds.data_structures;

public class DataItem {
    private String value;
    private long version;

    public DataItem(String value) {
        this.value = value;
        this.version = 0;
    }

    public DataItem(String value, long version) {
        this.value = value;
        this.version = version;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value, long version) {
        this.value = value;
        this.version = version;
    }

    public long getVersion() {
        return version;
    }
    
}
