package com.transdata;

public interface ProgressListener {
    void updateStage(String stage);

    void updateProgress(int percent, String detail);

    void updateStats(TransferStats stats);
}
