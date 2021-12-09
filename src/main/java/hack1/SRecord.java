package hack1;

public class SRecord {
    private final int id;
    private final String msg;

    public int getId() {
        return this.id;
    }

    SRecord(int id, String sg) {
        this.id = id;
        this.msg = sg;
    }

    @Override
    public String toString() {
        //hacky, after all.
        return "SRecord#" + id + "#" + msg;
    }
}
