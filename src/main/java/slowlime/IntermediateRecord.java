package slowlime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IntermediateRecord implements Writable {
    public double price = 0.0;
    public int qty = 0;

    public IntermediateRecord() {
    }

    public IntermediateRecord(double price, int qty) {
        this.price = price;
        this.qty = qty;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(price);
        out.writeInt(qty);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        price = in.readDouble();
        qty = in.readInt();
    }

    @Override
    public String toString() {
        return String.format("%f\t%d", price, qty);
    }
}
