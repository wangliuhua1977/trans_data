package com.transdata;

import org.apache.hc.client5.http.classic.CloseableHttpClient;
import org.apache.hc.core5.util.Timeout;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToolBar;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransDataFrame extends JFrame implements UiLogSink, ProgressListener {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final AppConfig config;
    private final JTextField startField = new JTextField(8);
    private final JTextField endField = new JTextField(8);
    private final JSpinner intervalValue = new JSpinner(new SpinnerNumberModel(180, 1, 86400, 1));
    private final JComboBox<String> intervalUnit = new JComboBox<>(new String[]{"Seconds", "Minutes"});
    private final JSpinner batchSizeField = new JSpinner(new SpinnerNumberModel(500, 1, 5000, 1));
    private final JCheckBox insecureCheck = new JCheckBox("HTTPS insecure");
    private final JButton runOnceButton = new JButton("立即执行一次");
    private final JButton startButton = new JButton("开始");
    private final JButton pauseButton = new JButton("暂停");
    private final JButton clearButton = new JButton("清空日志");

    private final JProgressBar progressBar = new JProgressBar(0, 100);
    private final JLabel stageLabel = new JLabel("Idle");
    private final JLabel statsLabel = new JLabel("Ready");
    private final JTextArea logArea = new JTextArea();

    private SchedulerService schedulerService;

    public TransDataFrame(AppConfig config) {
        this.config = config;
        setTitle("trans_data - 数据传输");
        setSize(980, 640);
        setLocationRelativeTo(null);
        setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        initLayout();
        initConfigValues();
        initActions();
    }

    public void startSchedulerOnLaunch() {
        if (config.isHttpsInsecure()) {
            log("INFO", "HTTPS insecure mode is enabled (trust-all + no hostname verification).");
        }
        schedulerService.start();
    }

    private void initLayout() {
        JPanel topPanel = new JPanel(new GridBagLayout());
        topPanel.setBorder(BorderFactory.createTitledBorder("配置"));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;

        gbc.gridx = 0;
        gbc.gridy = 0;
        topPanel.add(new JLabel("窗口开始"), gbc);
        gbc.gridx = 1;
        topPanel.add(startField, gbc);
        gbc.gridx = 2;
        topPanel.add(new JLabel("窗口结束"), gbc);
        gbc.gridx = 3;
        topPanel.add(endField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 1;
        topPanel.add(new JLabel("执行间隔"), gbc);
        gbc.gridx = 1;
        topPanel.add(intervalValue, gbc);
        gbc.gridx = 2;
        topPanel.add(intervalUnit, gbc);

        gbc.gridx = 3;
        topPanel.add(new JLabel("批大小"), gbc);
        gbc.gridx = 4;
        topPanel.add(batchSizeField, gbc);

        gbc.gridx = 5;
        topPanel.add(insecureCheck, gbc);

        JToolBar toolbar = new JToolBar();
        toolbar.setFloatable(false);
        toolbar.add(runOnceButton);
        toolbar.add(startButton);
        toolbar.add(pauseButton);
        toolbar.add(clearButton);

        JPanel progressPanel = new JPanel(new GridBagLayout());
        progressPanel.setBorder(BorderFactory.createTitledBorder("进度"));
        GridBagConstraints pgbc = new GridBagConstraints();
        pgbc.insets = new Insets(4, 4, 4, 4);
        pgbc.anchor = GridBagConstraints.WEST;
        pgbc.gridx = 0;
        pgbc.gridy = 0;
        pgbc.fill = GridBagConstraints.HORIZONTAL;
        pgbc.weightx = 1.0;
        progressBar.setStringPainted(true);
        progressPanel.add(progressBar, pgbc);

        pgbc.gridy = 1;
        progressPanel.add(stageLabel, pgbc);

        pgbc.gridy = 2;
        progressPanel.add(statsLabel, pgbc);

        logArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(logArea);
        scrollPane.setPreferredSize(new Dimension(900, 300));
        JPanel logPanel = new JPanel(new BorderLayout());
        logPanel.setBorder(BorderFactory.createTitledBorder("日志"));
        logPanel.add(scrollPane, BorderLayout.CENTER);

        JPanel centerPanel = new JPanel(new BorderLayout());
        centerPanel.add(progressPanel, BorderLayout.NORTH);
        centerPanel.add(logPanel, BorderLayout.CENTER);

        JPanel northPanel = new JPanel(new BorderLayout());
        northPanel.add(topPanel, BorderLayout.CENTER);
        northPanel.add(toolbar, BorderLayout.SOUTH);

        getContentPane().setLayout(new BorderLayout());
        getContentPane().add(northPanel, BorderLayout.NORTH);
        getContentPane().add(centerPanel, BorderLayout.CENTER);

        schedulerService = new SchedulerService(config, this::createJob, this);

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                schedulerService.stop();
            }
        });
    }

    private void initConfigValues() {
        startField.setText(config.getWindowStart().format(TIME_FORMATTER));
        endField.setText(config.getWindowEnd().format(TIME_FORMATTER));
        intervalValue.setValue(config.getIntervalSeconds());
        intervalUnit.setSelectedIndex(0);
        batchSizeField.setValue(config.getBatchSize());
        insecureCheck.setSelected(config.isHttpsInsecure());
    }

    private void initActions() {
        runOnceButton.addActionListener(event -> schedulerService.triggerOnce());
        startButton.addActionListener(event -> {
            config.setScheduleEnabled(true);
            config.save();
            schedulerService.start();
        });
        pauseButton.addActionListener(event -> schedulerService.stop());
        clearButton.addActionListener(event -> logArea.setText(""));

        DocumentListener documentListener = new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) {
                applyConfigChanges();
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                applyConfigChanges();
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                applyConfigChanges();
            }
        };
        startField.getDocument().addDocumentListener(documentListener);
        endField.getDocument().addDocumentListener(documentListener);

        intervalValue.addChangeListener(event -> applyConfigChanges());
        intervalUnit.addActionListener(event -> applyConfigChanges());
        batchSizeField.addChangeListener(event -> applyConfigChanges());
        insecureCheck.addActionListener(event -> applyConfigChanges());
    }

    private synchronized void applyConfigChanges() {
        try {
            LocalTime start = LocalTime.parse(startField.getText().trim(), TIME_FORMATTER);
            LocalTime end = LocalTime.parse(endField.getText().trim(), TIME_FORMATTER);
            config.setWindowStart(start);
            config.setWindowEnd(end);
        } catch (Exception ignored) {
            // keep last valid configuration
        }
        int interval = (Integer) intervalValue.getValue();
        String unit = (String) intervalUnit.getSelectedItem();
        int seconds = "Minutes".equals(unit) ? interval * 60 : interval;
        config.setIntervalSeconds(seconds);
        config.setBatchSize((Integer) batchSizeField.getValue());
        config.setHttpsInsecure(insecureCheck.isSelected());
        config.save();
        if (config.isScheduleEnabled()) {
            schedulerService.reschedule();
        }
    }

    private Runnable createJob(AtomicBoolean cancelled) {
        Timeout connectTimeout = Timeout.ofSeconds(10);
        Timeout responseTimeout = Timeout.ofSeconds(30);
        HttpClientFactory factory = new HttpClientFactory(config.isHttpsInsecure(), connectTimeout, responseTimeout);
        CloseableHttpClient client = factory.createClient();
        SourceClient sourceClient = new SourceClient(client, connectTimeout, responseTimeout);
        AsyncSqlClient asyncSqlClient = new AsyncSqlClient(client, connectTimeout, responseTimeout);
        SqlBuilder sqlBuilder = new SqlBuilder();
        TransferJob job = new TransferJob(config, sourceClient, asyncSqlClient, sqlBuilder, this, this, cancelled);
        return () -> {
            try {
                job.run();
            } finally {
                try {
                    client.close();
                } catch (Exception ignored) {
                    // ignore close failures
                }
            }
        };
    }

    @Override
    public void log(String level, String message) {
        SwingUtilities.invokeLater(() -> {
            String timestamp = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            logArea.append(timestamp + " [" + level + "] " + message + "\n");
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    @Override
    public void updateStage(String stage) {
        SwingUtilities.invokeLater(() -> stageLabel.setText("Stage: " + stage));
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
            String label = "Records: " + stats.getTotalRecords()
                    + ", Groups: " + stats.getCurrentGroup() + "/" + stats.getTotalGroups()
                    + ", Batch: " + stats.getCurrentBatch() + "/" + stats.getTotalBatches();
            if (stats.getJobId() != null && !stats.getJobId().isBlank()) {
                label += ", jobId=" + stats.getJobId();
            }
            statsLabel.setText(label);
        });
    }
}
