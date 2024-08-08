set global local_infile=1;
show variables like 'local_infile';

use tpch;
load data local infile '/Users/didi/flink/flink_query_processor/src/main/resources/data/customer.tbl' into table customer fields terminated by '|' lines terminated by '|\n';
load data local infile '/Users/didi/flink/flink_query_processor/src/main/resources/data/lineitem.tbl' into table lineitem fields terminated by '|' lines terminated by '|\n';
load data local infile '/Users/didi/flink/flink_query_processor/src/main/resources/data/nation.tbl' into table nation fields terminated by '|' lines terminated by '|\n';
load data local infile '/Users/didi/flink/flink_query_processor/src/main/resources/data/orders.tbl' into table orders fields terminated by '|' lines terminated by '|\n';
load data local infile '/Users/didi/flink/flink_query_processor/src/main/resources/data/part.tbl' into table part fields terminated by '|' lines terminated by '|\n';
load data local infile '/Users/didi/flink/flink_query_processor/src/main/resources/data/partsupp.tbl' into table partsupp fields terminated by '|' lines terminated by '|\n';
load data local infile '/Users/didi/flink/flink_query_processor/src/main/resources/data/region.tbl' into table region fields terminated by '|' lines terminated by '|\n';
load data local infile '/Users/didi/flink/flink_query_processor/src/main/resources/data/supplier.tbl' into table supplier fields terminated by '|' lines terminated by '|\n';
