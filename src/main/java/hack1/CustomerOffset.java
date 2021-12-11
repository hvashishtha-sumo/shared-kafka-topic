package hack1;

public class CustomerOffset {
    final int customerId;
    final long offset;

    CustomerOffset(final int cid, final long offset) {
        this.customerId = cid;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "CustomerOffset{" +
                "customerId=" + customerId +
                ", offset=" + offset +
                '}';
    }

    public String toString1() {
        return "CustomerOffset#" + customerId + "#" + offset;
    }

    public CustomerOffset fromString1(final String str) {
        final String[] mems = str.split("#");
        if (mems.length != 3) {
            throw new IllegalArgumentException("not a valid customerOffset" + str);
        }
        return new CustomerOffset(Integer.parseInt(mems[1]), Long.parseLong(mems[2]));
    }
}
