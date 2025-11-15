package ds.data_structures;

public class ResponseResult {
    private final boolean success;
    private final String value;

    public ResponseResult(boolean success, String value) {
        this.success = success;
        this.value = value;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getValue() {
        return value;
    }
}