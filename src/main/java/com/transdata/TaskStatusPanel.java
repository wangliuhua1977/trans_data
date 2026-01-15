package com.transdata;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

public class TaskStatusPanel extends JPanel implements ProgressListener {
    private final JProgressBar progressBar = new JProgressBar(0, 100);
    private final JLabel stageLabel = new JLabel("空闲");
    private final JLabel statsLabel = new JLabel("就绪");
    private final JLabel pollingLabel = new JLabel("轮询：-");

    public TaskStatusPanel() {
        super(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1.0;
        progressBar.setStringPainted(true);
        add(progressBar, gbc);

        gbc.gridy = 1;
        add(stageLabel, gbc);

        gbc.gridy = 2;
        add(statsLabel, gbc);

        gbc.gridy = 3;
        add(pollingLabel, gbc);
    }

    @Override
    public void updateStage(String stage) {
        SwingUtilities.invokeLater(() -> stageLabel.setText("阶段：" + stage));
    }

    @Override
    public void updateProgress(int percent, String detail) {
        SwingUtilities.invokeLater(() -> {
            progressBar.setValue(percent);
            progressBar.setString(detail);
        });
    }

    @Override
    public void updateStats(TransferStats stats) {
        SwingUtilities.invokeLater(() -> {
            String label = "记录数：" + stats.getTotalRecords()
                    + "，分段：" + stats.getCurrentBatch() + "/" + stats.getTotalBatches();
            if (stats.getJobId() != null && !stats.getJobId().isBlank()) {
                label += "，作业ID=" + stats.getJobId();
            }
            statsLabel.setText(label);
        });
    }

    @Override
    public void updatePolling(String status, long elapsedMillis, Integer progressPercent) {
        SwingUtilities.invokeLater(() -> {
            String progress = progressPercent == null || progressPercent < 0 ? "-" : progressPercent + "%";
            String label = "轮询：状态=" + status + "，耗时=" + String.format("%.1fs", elapsedMillis / 1000.0)
                    + "，进度=" + progress;
            pollingLabel.setText(label);
        });
    }
}
