package com.collective.batchelor.util;

import java.util.List;


public interface BatchHandler<T> {
    boolean handle(List<T> batch);

    void done();
}
