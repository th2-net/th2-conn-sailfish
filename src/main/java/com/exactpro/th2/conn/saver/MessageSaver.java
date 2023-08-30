package com.exactpro.th2.conn.saver;

import com.exactpro.th2.conn.ConnectivityBatch;

import java.io.IOException;

public interface MessageSaver {
    void save(ConnectivityBatch batch) throws IOException;
}
