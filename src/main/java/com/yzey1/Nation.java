package com.yzey1;

public class Nation extends DataTuple {
    private String N_NAME;
    private int N_REGIONKEY; // Foreign key
    private String N_COMMENT;

    public Nation(int N_NATIONKEY, String N_NAME, int N_REGIONKEY, String N_COMMENT) {
        super(N_NATIONKEY);
        this.N_NAME = N_NAME;
        this.N_REGIONKEY = N_REGIONKEY;
        this.N_COMMENT = N_COMMENT;
    }

    // Getters and setters
}