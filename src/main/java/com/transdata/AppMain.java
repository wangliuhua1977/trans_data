package com.transdata;

import javax.swing.SwingUtilities;

public final class AppMain {
    private AppMain() {
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            AppConfig config = AppConfig.load();
            TransDataFrame frame = new TransDataFrame(config);
            frame.setVisible(true);
            frame.startSchedulerOnLaunch();
        });
    }
}
