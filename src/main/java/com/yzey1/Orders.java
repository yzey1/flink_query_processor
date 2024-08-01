package com.yzey1;

public class Orders extends DataTuple {
    private int O_CUSTKEY; // Foreign key
    private String O_ORDERSTATUS;
    private double O_TOTALPRICE;
    private String O_ORDERDATE;
    private String O_ORDERPRIORITY;
    private String O_CLERK;
    private int O_SHIPPRIORITY;
    private String O_COMMENT;

    public Orders(int O_ORDERKEY, int O_CUSTKEY, String O_ORDERSTATUS, double O_TOTALPRICE, String O_ORDERDATE, String O_ORDERPRIORITY, String O_CLERK, int O_SHIPPRIORITY, String O_COMMENT) {
        super(O_ORDERKEY);
        this.O_CUSTKEY = O_CUSTKEY;
        this.O_ORDERSTATUS = O_ORDERSTATUS;
        this.O_TOTALPRICE = O_TOTALPRICE;
        this.O_ORDERDATE = O_ORDERDATE;
        this.O_ORDERPRIORITY = O_ORDERPRIORITY;
        this.O_CLERK = O_CLERK;
        this.O_SHIPPRIORITY = O_SHIPPRIORITY;
        this.O_COMMENT = O_COMMENT;
    }

    // Getters and setters
}