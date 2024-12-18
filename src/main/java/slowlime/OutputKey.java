package slowlime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class OutputKey implements WritableComparable<OutputKey> {
    public Text key = new Text();
    public double revenue;

    public OutputKey() {
    }

    public OutputKey(Text key, double revenue) {
        this.key = key;
        this.revenue = revenue;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        out.writeDouble(revenue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        revenue = in.readDouble();
    }

    @Override
    public int compareTo(OutputKey other) {
        int r = -Double.compare(revenue, other.revenue);

        if (r == 0) {
            return key.toString().compareTo(other.key.toString());
        }

        return r;
    }

    @Override
    public String toString() {
        return key.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        long temp;
        temp = Double.doubleToLongBits(revenue);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        OutputKey other = (OutputKey) obj;
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        if (Double.doubleToLongBits(revenue) != Double.doubleToLongBits(other.revenue))
            return false;
        return true;
    }

}
