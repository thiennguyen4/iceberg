package dev.onemount.iceburg.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class IcebergTableConfig {

    @Value("${iceberg.table.write.format.default:avro}")
    private String writeFormat;

    @Value("${iceberg.table.write.avro.compression-codec:snappy}")
    private String avroCompressionCodec;

    @Value("${iceberg.table.write.parquet.compression-codec:zstd}")
    private String parquetCompressionCodec;

    public String getTableProperties() {
        if ("avro".equalsIgnoreCase(writeFormat)) {
            return String.format(
                "TBLPROPERTIES ('write.format.default'='avro', 'write.avro.compression-codec'='%s')",
                avroCompressionCodec
            );
        } else if ("parquet".equalsIgnoreCase(writeFormat)) {
            return String.format(
                "TBLPROPERTIES ('write.format.default'='parquet', 'write.parquet.compression-codec'='%s')",
                parquetCompressionCodec
            );
        } else {
            return String.format("TBLPROPERTIES ('write.format.default'='%s')", writeFormat);
        }
    }
}
