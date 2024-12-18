package slowlime;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupingMapper extends Mapper<Object, Text, Text, IntermediateRecord> {
    private static class InputRecord {
        public long txId;
        public long productId;
        public String category;
        public double price;
        public int qty;

        public InputRecord(long txId, long productId, String category, double price, int qty) {
            this.txId = txId;
            this.productId = productId;
            this.category = category;
            this.price = price;
            this.qty = qty;
        }

        public static InputRecord fromFields(String[] fields) {
            var txId = Long.parseLong(fields[0]);
            var productId = Long.parseLong(fields[1]);
            var category = fields[2];
            var price = Double.parseDouble(fields[3]);
            var qty = Integer.parseInt(fields[4]);

            return new InputRecord(txId, productId, category, price, qty);
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("transaction_id,")) {
            return;
        }

        var fields = value.toString().split(",");
        var record = InputRecord.fromFields(fields);
        var outKey = new Text(record.category);
        var outValue = new IntermediateRecord(record.price, record.qty);
        context.write(outKey, outValue);
    }
}
