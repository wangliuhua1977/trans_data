package com.transdata;

import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import java.awt.BorderLayout;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Deque;

public class UiLogPanel extends javax.swing.JPanel implements UiLogSink {
    private static final int MAX_LOG_LINES = 1000;
    private static final int SCROLL_BOTTOM_THRESHOLD = 20;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final JTextArea logArea = new JTextArea();
    private final Deque<String> logLines = new ArrayDeque<>(MAX_LOG_LINES);
    private final JScrollPane scrollPane;

    public UiLogPanel() {
        super(new BorderLayout());
        logArea.setEditable(false);
        scrollPane = new JScrollPane(logArea);
        add(scrollPane, BorderLayout.CENTER);
    }

    @Override
    public void log(String level, String message) {
        SwingUtilities.invokeLater(() -> {
            String timestamp = LocalDateTime.now().format(TIME_FORMATTER);
            boolean shouldScroll = isScrollNearBottom();
            String line = timestamp + " [" + level + "] " + message;
            logLines.addLast(line);
            while (logLines.size() > MAX_LOG_LINES) {
                logLines.removeFirst();
            }
            StringBuilder builder = new StringBuilder();
            for (String entry : logLines) {
                builder.append(entry).append("\n");
            }
            logArea.setText(builder.toString());
            if (shouldScroll) {
                logArea.setCaretPosition(logArea.getDocument().getLength());
            }
        });
    }

    public void clear() {
        SwingUtilities.invokeLater(() -> {
            logLines.clear();
            logArea.setText("");
        });
    }

    private boolean isScrollNearBottom() {
        JScrollBar verticalBar = scrollPane.getVerticalScrollBar();
        int value = verticalBar.getValue();
        int extent = verticalBar.getModel().getExtent();
        int maximum = verticalBar.getMaximum();
        return value + extent >= maximum - SCROLL_BOTTOM_THRESHOLD;
    }
}
