package com.transdata;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.util.Timeout;

import javax.swing.BorderFactory;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransDataFrame extends JFrame {
    private final AppConfig config;
    private final TaskStore taskStore;
    private final DefaultListModel<TaskDefinition> taskListModel = new DefaultListModel<>();
    private final JList<TaskDefinition> taskList = new JList<>(taskListModel);
    private final Map<String, UiLogPanel> logPanels = new HashMap<>();
    private final Map<String, TaskStatusPanel> statusPanels = new HashMap<>();
    private final Map<String, TaskScheduler> schedulers = new HashMap<>();
    private final ExecutorService backgroundExecutor = Executors.newFixedThreadPool(2, r -> {
        Thread thread = new Thread(r, "trans-data-ui-worker");
        thread.setDaemon(true);
        return thread;
    });

    private TaskDefinition selectedTask;
    private boolean updatingFields = false;

    private final JTextField taskIdField = new JTextField(32);
    private final JTextField taskNameField = new JTextField(24);
    private final JCheckBox enabledCheck = new JCheckBox("启用任务");
    private final JTextField sourceUrlField = new JTextField(32);
    private final JTextArea sourceBodyArea = new JTextArea(6, 32);
    private final JSpinner batchSizeSpinner = new JSpinner(new SpinnerNumberModel(100, 1, 10000, 1));

    private final JSpinner intervalValue = new JSpinner(new SpinnerNumberModel(180, 1, 86400, 1));
    private final JTextField windowStartField = new JTextField(8);
    private final JTextField windowEndField = new JTextField(8);
    private final JSpinner maxRetriesSpinner = new JSpinner(new SpinnerNumberModel(3, 1, 20, 1));
    private final JSpinner leaseSecondsSpinner = new JSpinner(new SpinnerNumberModel(300, 30, 3600, 30));

    private final JTextField targetSchemaField = new JTextField(10);
    private final JTextField targetTableField = new JTextField(20);
    private final JTextArea createTableArea = new JTextArea(6, 32);
    private final JComboBox<InsertMode> insertModeCombo = new JComboBox<>(InsertMode.values());
    private final JTextArea insertTemplateArea = new JTextArea(6, 32);
    private final JTextField conflictTargetField = new JTextField(20);
    private final JTextField scopeKeyField = new JTextField(24);
    private final JTextField naturalKeyField = new JTextField(24);
    private final JTextField distinctKeyField = new JTextField(24);

    private final JTextArea testResponseArea = new JTextArea();
    private final JTextField testStatusField = new JTextField(20);
    private final JTextField testElapsedField = new JTextField(20);
    private final JTextField testSizeField = new JTextField(20);

    private JPanel logsContainer;

    public TransDataFrame(AppConfig config) {
        this.config = config;
        this.taskStore = new TaskStore(config);
        setTitle("trans_data - 数据传输");
        setSize(1200, 720);
        setLocationRelativeTo(null);
        setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        initLayout();
        initActions();
        loadTasks();
    }

    public void startSchedulerOnLaunch() {
        for (TaskDefinition task : getTasks()) {
            if (task.isEnabled()) {
                ensureScheduler(task).start();
            }
        }
    }

    private void initLayout() {
        JSplitPane splitPane = new JSplitPane();
        splitPane.setResizeWeight(0.2);
        splitPane.setLeftComponent(buildTaskListPanel());
        splitPane.setRightComponent(buildDetailPanel());
        getContentPane().setLayout(new BorderLayout());
        getContentPane().add(splitPane, BorderLayout.CENTER);

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                schedulers.values().forEach(TaskScheduler::stop);
                backgroundExecutor.shutdownNow();
            }
        });
    }

    private JPanel buildTaskListPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(BorderFactory.createTitledBorder("任务列表"));
        taskList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        taskList.setCellRenderer(new DefaultListCellRenderer() {
            @Override
            public java.awt.Component getListCellRendererComponent(JList<?> list, Object value, int index,
                                                                   boolean isSelected, boolean cellHasFocus) {
                super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
                if (value instanceof TaskDefinition task) {
                    setText(task.displayName());
                }
                return this;
            }
        });
        panel.add(new JScrollPane(taskList), BorderLayout.CENTER);

        JButton addButton = new JButton("新增");
        JButton deleteButton = new JButton("删除");
        JButton toggleButton = new JButton("启用/停用");
        JToolBar toolBar = new JToolBar();
        toolBar.setFloatable(false);
        toolBar.add(addButton);
        toolBar.add(deleteButton);
        toolBar.add(toggleButton);
        panel.add(toolBar, BorderLayout.SOUTH);

        addButton.addActionListener(event -> addTask());
        deleteButton.addActionListener(event -> deleteSelectedTask());
        toggleButton.addActionListener(event -> toggleTaskEnabled());

        taskList.addListSelectionListener(event -> {
            if (!event.getValueIsAdjusting()) {
                TaskDefinition task = taskList.getSelectedValue();
                setSelectedTask(task);
            }
        });
        return panel;
    }

    private JPanel buildDetailPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(BorderFactory.createTitledBorder("任务详情"));

        JToolBar toolbar = new JToolBar();
        toolbar.setFloatable(false);
        JButton runOnceButton = new JButton("立即执行一次");
        JButton startButton = new JButton("启动调度");
        JButton stopButton = new JButton("停止调度");
        toolbar.add(runOnceButton);
        toolbar.add(startButton);
        toolbar.add(stopButton);

        runOnceButton.addActionListener(event -> runSelectedTaskOnce());
        startButton.addActionListener(event -> startSelectedTask());
        stopButton.addActionListener(event -> stopSelectedTask());

        JTabbedPane tabs = new JTabbedPane();
        tabs.addTab("基础", buildBasicPanel());
        tabs.addTab("调度", buildSchedulePanel());
        tabs.addTab("目标与插入", buildTargetPanel());
        tabs.addTab("测试", buildTestPanel());
        logsContainer = buildLogsPanel();
        tabs.addTab("日志", logsContainer);

        panel.add(toolbar, BorderLayout.NORTH);
        panel.add(tabs, BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildBasicPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;

        taskIdField.setEditable(false);

        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(new javax.swing.JLabel("任务ID"), gbc);
        gbc.gridx = 1;
        panel.add(taskIdField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 1;
        panel.add(new javax.swing.JLabel("任务名称"), gbc);
        gbc.gridx = 1;
        panel.add(taskNameField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 2;
        panel.add(new javax.swing.JLabel("启用状态"), gbc);
        gbc.gridx = 1;
        panel.add(enabledCheck, gbc);

        gbc.gridx = 0;
        gbc.gridy = 3;
        panel.add(new javax.swing.JLabel("源 POST URL"), gbc);
        gbc.gridx = 1;
        panel.add(sourceUrlField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 4;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        panel.add(new javax.swing.JLabel("请求体"), gbc);
        gbc.gridx = 1;
        panel.add(new JScrollPane(sourceBodyArea), gbc);

        gbc.gridx = 0;
        gbc.gridy = 5;
        gbc.anchor = GridBagConstraints.WEST;
        panel.add(new javax.swing.JLabel("分段大小"), gbc);
        gbc.gridx = 1;
        panel.add(batchSizeSpinner, gbc);

        return panel;
    }

    private JPanel buildSchedulePanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;

        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(new javax.swing.JLabel("间隔（秒）"), gbc);
        gbc.gridx = 1;
        panel.add(intervalValue, gbc);

        gbc.gridx = 0;
        gbc.gridy = 1;
        panel.add(new javax.swing.JLabel("窗口开始"), gbc);
        gbc.gridx = 1;
        panel.add(windowStartField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 2;
        panel.add(new javax.swing.JLabel("窗口结束"), gbc);
        gbc.gridx = 1;
        panel.add(windowEndField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 3;
        panel.add(new javax.swing.JLabel("最大重试"), gbc);
        gbc.gridx = 1;
        panel.add(maxRetriesSpinner, gbc);

        gbc.gridx = 0;
        gbc.gridy = 4;
        panel.add(new javax.swing.JLabel("锁租约秒数"), gbc);
        gbc.gridx = 1;
        panel.add(leaseSecondsSpinner, gbc);

        return panel;
    }

    private JPanel buildTargetPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;

        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(new javax.swing.JLabel("目标 Schema"), gbc);
        gbc.gridx = 1;
        panel.add(targetSchemaField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 1;
        panel.add(new javax.swing.JLabel("目标表"), gbc);
        gbc.gridx = 1;
        panel.add(targetTableField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        panel.add(new javax.swing.JLabel("建表 SQL"), gbc);
        gbc.gridx = 1;
        panel.add(new JScrollPane(createTableArea), gbc);

        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.anchor = GridBagConstraints.WEST;
        panel.add(new javax.swing.JLabel("插入模式"), gbc);
        gbc.gridx = 1;
        panel.add(insertModeCombo, gbc);

        gbc.gridx = 0;
        gbc.gridy = 4;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        panel.add(new javax.swing.JLabel("插入模板"), gbc);
        gbc.gridx = 1;
        panel.add(new JScrollPane(insertTemplateArea), gbc);

        gbc.gridx = 0;
        gbc.gridy = 5;
        gbc.anchor = GridBagConstraints.WEST;
        panel.add(new javax.swing.JLabel("冲突目标"), gbc);
        gbc.gridx = 1;
        panel.add(conflictTargetField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 6;
        panel.add(new javax.swing.JLabel("范围键字段"), gbc);
        gbc.gridx = 1;
        panel.add(scopeKeyField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 7;
        panel.add(new javax.swing.JLabel("自然键字段"), gbc);
        gbc.gridx = 1;
        panel.add(naturalKeyField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 8;
        panel.add(new javax.swing.JLabel("去重字段"), gbc);
        gbc.gridx = 1;
        panel.add(distinctKeyField, gbc);

        return panel;
    }

    private JPanel buildTestPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        JPanel infoPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        infoPanel.add(new javax.swing.JLabel("HTTP 状态"), gbc);
        gbc.gridx = 1;
        testStatusField.setEditable(false);
        infoPanel.add(testStatusField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 1;
        infoPanel.add(new javax.swing.JLabel("耗时"), gbc);
        gbc.gridx = 1;
        testElapsedField.setEditable(false);
        infoPanel.add(testElapsedField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 2;
        infoPanel.add(new javax.swing.JLabel("响应大小"), gbc);
        gbc.gridx = 1;
        testSizeField.setEditable(false);
        infoPanel.add(testSizeField, gbc);

        JButton testButton = new JButton("测试源接口");
        testButton.addActionListener(event -> runTest());

        JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(infoPanel, BorderLayout.CENTER);
        topPanel.add(testButton, BorderLayout.EAST);

        testResponseArea.setEditable(false);
        testResponseArea.setLineWrap(false);
        JScrollPane scrollPane = new JScrollPane(testResponseArea);
        scrollPane.setPreferredSize(new Dimension(800, 400));

        panel.add(topPanel, BorderLayout.NORTH);
        panel.add(scrollPane, BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildLogsPanel() {
        return new JPanel(new BorderLayout());
    }

    private void initActions() {
        DocumentListener updateListener = new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) {
                applyChanges();
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                applyChanges();
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                applyChanges();
            }
        };
        taskNameField.getDocument().addDocumentListener(updateListener);
        sourceUrlField.getDocument().addDocumentListener(updateListener);
        sourceBodyArea.getDocument().addDocumentListener(updateListener);
        windowStartField.getDocument().addDocumentListener(updateListener);
        windowEndField.getDocument().addDocumentListener(updateListener);
        targetSchemaField.getDocument().addDocumentListener(updateListener);
        targetTableField.getDocument().addDocumentListener(updateListener);
        createTableArea.getDocument().addDocumentListener(updateListener);
        insertTemplateArea.getDocument().addDocumentListener(updateListener);
        conflictTargetField.getDocument().addDocumentListener(updateListener);
        scopeKeyField.getDocument().addDocumentListener(updateListener);
        naturalKeyField.getDocument().addDocumentListener(updateListener);
        distinctKeyField.getDocument().addDocumentListener(updateListener);

        enabledCheck.addActionListener(event -> applyChanges());
        batchSizeSpinner.addChangeListener(event -> applyChanges());
        intervalValue.addChangeListener(event -> applyChanges());
        maxRetriesSpinner.addChangeListener(event -> applyChanges());
        leaseSecondsSpinner.addChangeListener(event -> applyChanges());
        insertModeCombo.addActionListener(event -> applyChanges());
    }

    private void loadTasks() {
        List<TaskDefinition> tasks = taskStore.loadTasks(uiLogForApp());
        taskListModel.clear();
        for (TaskDefinition task : tasks) {
            taskListModel.addElement(task);
            ensureLogPanel(task);
            ensureStatusPanel(task);
        }
        if (!taskListModel.isEmpty()) {
            taskList.setSelectedIndex(0);
        }
    }

    private List<TaskDefinition> getTasks() {
        List<TaskDefinition> tasks = new ArrayList<>();
        for (int i = 0; i < taskListModel.size(); i++) {
            tasks.add(taskListModel.get(i));
        }
        return tasks;
    }

    private void addTask() {
        TaskDefinition task = new TaskDefinition();
        task.setTaskId(UUID.randomUUID().toString());
        task.setTaskName("新任务");
        task.setEnabled(false);
        task.setSourcePostUrl(config.getSourceUrl());
        task.setSourcePostBody(config.getSourceBody());
        task.setBatchSize(100);
        SchedulePolicy policy = new SchedulePolicy();
        policy.setIntervalSeconds(180);
        policy.setWindowStart("00:00:00");
        policy.setWindowEnd("23:59:59");
        policy.setMaxRetries(3);
        policy.setLeaseSeconds(300);
        task.setSchedulePolicy(policy);
        TargetConfig target = new TargetConfig();
        target.setTargetSchema("leshan");
        target.setInsertMode(InsertMode.APPEND);
        task.setTargetConfig(target);
        taskListModel.addElement(task);
        ensureLogPanel(task);
        ensureStatusPanel(task);
        saveTasks();
        taskList.setSelectedValue(task, true);
    }

    private void deleteSelectedTask() {
        if (selectedTask == null) {
            return;
        }
        int confirm = JOptionPane.showConfirmDialog(this, "确认删除当前任务？", "确认",
                JOptionPane.YES_NO_OPTION);
        if (confirm != JOptionPane.YES_OPTION) {
            return;
        }
        TaskScheduler scheduler = schedulers.remove(selectedTask.getTaskId());
        if (scheduler != null) {
            scheduler.stop();
        }
        logPanels.remove(selectedTask.getTaskId());
        statusPanels.remove(selectedTask.getTaskId());
        taskListModel.removeElement(selectedTask);
        saveTasks();
        if (!taskListModel.isEmpty()) {
            taskList.setSelectedIndex(0);
        } else {
            setSelectedTask(null);
        }
    }

    private void toggleTaskEnabled() {
        if (selectedTask == null) {
            return;
        }
        selectedTask.setEnabled(!selectedTask.isEnabled());
        saveTasks();
        taskList.repaint();
        if (selectedTask.isEnabled()) {
            ensureScheduler(selectedTask).start();
        } else {
            stopSelectedTask();
        }
        updateFieldsFromSelected();
    }

    private void setSelectedTask(TaskDefinition task) {
        selectedTask = task;
        updateFieldsFromSelected();
        updateLogsTab();
    }

    private void updateFieldsFromSelected() {
        updatingFields = true;
        try {
            if (selectedTask == null) {
                taskIdField.setText("");
                taskNameField.setText("");
                enabledCheck.setSelected(false);
                sourceUrlField.setText("");
                sourceBodyArea.setText("");
                batchSizeSpinner.setValue(100);
                intervalValue.setValue(180);
                windowStartField.setText("");
                windowEndField.setText("");
                maxRetriesSpinner.setValue(3);
                leaseSecondsSpinner.setValue(300);
                targetSchemaField.setText("");
                targetTableField.setText("");
                createTableArea.setText("");
                insertModeCombo.setSelectedItem(InsertMode.APPEND);
                insertTemplateArea.setText("");
                conflictTargetField.setText("");
                scopeKeyField.setText("");
                naturalKeyField.setText("");
                distinctKeyField.setText("");
                return;
            }
            taskIdField.setText(selectedTask.getTaskId());
            taskNameField.setText(selectedTask.getTaskName());
            enabledCheck.setSelected(selectedTask.isEnabled());
            sourceUrlField.setText(selectedTask.getSourcePostUrl());
            sourceBodyArea.setText(selectedTask.getSourcePostBody());
            batchSizeSpinner.setValue(selectedTask.getBatchSize());

            SchedulePolicy schedule = selectedTask.getSchedulePolicy();
            intervalValue.setValue(schedule.getIntervalSeconds());
            windowStartField.setText(schedule.getWindowStart());
            windowEndField.setText(schedule.getWindowEnd());
            maxRetriesSpinner.setValue(schedule.getMaxRetries());
            leaseSecondsSpinner.setValue(schedule.getLeaseSeconds());

            TargetConfig target = selectedTask.getTargetConfig();
            targetSchemaField.setText(target.getTargetSchema());
            targetTableField.setText(target.getTargetTable());
            createTableArea.setText(target.getCreateTableSql());
            insertModeCombo.setSelectedItem(target.getInsertMode());
            insertTemplateArea.setText(target.getInsertTemplate());
            conflictTargetField.setText(target.getConflictTarget());
            scopeKeyField.setText(String.join(",", target.getAuditSettings().getScopeKeyFields()));
            naturalKeyField.setText(String.join(",", target.getAuditSettings().getNaturalKeyFields()));
            distinctKeyField.setText(target.getAuditSettings().getDistinctKeyJsonField());
        } finally {
            updatingFields = false;
        }
    }

    private void applyChanges() {
        if (updatingFields || selectedTask == null) {
            return;
        }
        selectedTask.setTaskName(taskNameField.getText().trim());
        selectedTask.setEnabled(enabledCheck.isSelected());
        selectedTask.setSourcePostUrl(sourceUrlField.getText().trim());
        selectedTask.setSourcePostBody(sourceBodyArea.getText());
        selectedTask.setBatchSize((Integer) batchSizeSpinner.getValue());

        SchedulePolicy schedule = selectedTask.getSchedulePolicy();
        schedule.setIntervalSeconds((Integer) intervalValue.getValue());
        schedule.setWindowStart(windowStartField.getText().trim());
        schedule.setWindowEnd(windowEndField.getText().trim());
        schedule.setMaxRetries((Integer) maxRetriesSpinner.getValue());
        schedule.setLeaseSeconds((Integer) leaseSecondsSpinner.getValue());

        TargetConfig target = selectedTask.getTargetConfig();
        target.setTargetSchema(targetSchemaField.getText().trim());
        target.setTargetTable(targetTableField.getText().trim());
        target.setCreateTableSql(createTableArea.getText());
        target.setInsertMode((InsertMode) insertModeCombo.getSelectedItem());
        target.setInsertTemplate(insertTemplateArea.getText());
        target.setConflictTarget(conflictTargetField.getText().trim());
        target.getAuditSettings().setScopeKeyFields(splitFields(scopeKeyField.getText()));
        target.getAuditSettings().setNaturalKeyFields(splitFields(naturalKeyField.getText()));
        target.getAuditSettings().setDistinctKeyJsonField(distinctKeyField.getText().trim());

        saveTasks();
        taskList.repaint();
        TaskScheduler scheduler = ensureScheduler(selectedTask);
        if (selectedTask.isEnabled()) {
            scheduler.reschedule();
        } else {
            scheduler.stop();
        }
    }

    private List<String> splitFields(String text) {
        List<String> fields = new ArrayList<>();
        if (text == null || text.isBlank()) {
            return fields;
        }
        for (String part : text.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isBlank()) {
                fields.add(trimmed);
            }
        }
        return fields;
    }

    private void saveTasks() {
        taskStore.saveTasks(getTasks(), uiLogForApp());
    }

    private void updateLogsTab() {
        logsContainer.removeAll();
        if (selectedTask != null) {
            JPanel panel = new JPanel(new BorderLayout());
            TaskStatusPanel statusPanel = ensureStatusPanel(selectedTask);
            UiLogPanel logPanel = ensureLogPanel(selectedTask);
            JButton clearButton = new JButton("清空日志");
            clearButton.addActionListener(event -> logPanel.clear());
            JPanel top = new JPanel(new BorderLayout());
            top.add(statusPanel, BorderLayout.CENTER);
            top.add(clearButton, BorderLayout.EAST);
            panel.add(top, BorderLayout.NORTH);
            panel.add(logPanel, BorderLayout.CENTER);
            logsContainer.add(panel, BorderLayout.CENTER);
        }
        logsContainer.revalidate();
        logsContainer.repaint();
    }

    private void runSelectedTaskOnce() {
        if (selectedTask == null) {
            return;
        }
        ensureScheduler(selectedTask).triggerOnce();
    }

    private void startSelectedTask() {
        if (selectedTask == null) {
            return;
        }
        selectedTask.setEnabled(true);
        saveTasks();
        ensureScheduler(selectedTask).start();
        updateFieldsFromSelected();
        taskList.repaint();
    }

    private void stopSelectedTask() {
        if (selectedTask == null) {
            return;
        }
        TaskScheduler scheduler = ensureScheduler(selectedTask);
        scheduler.stop();
    }

    private void runTest() {
        if (selectedTask == null) {
            return;
        }
        UiLogPanel logPanel = ensureLogPanel(selectedTask);
        testStatusField.setText("测试中...");
        testElapsedField.setText("");
        testSizeField.setText("");
        testResponseArea.setText("");
        backgroundExecutor.submit(() -> {
            try {
                Timeout connectTimeout = Timeout.ofSeconds(10);
                Timeout responseTimeout = Timeout.ofSeconds(30);
                HttpClientFactory factory = new HttpClientFactory(config.isHttpsInsecure(), connectTimeout, responseTimeout);
                try (CloseableHttpClient client = factory.createClient()) {
                    SourceClient sourceClient = new SourceClient(client, connectTimeout, responseTimeout);
                    SourceFetchResult result = sourceClient.testPost(selectedTask, config);
                    SwingUtilities.invokeLater(() -> {
                        testStatusField.setText(String.valueOf(result.getStatusCode()));
                        testElapsedField.setText(result.getElapsedMillis() + " ms");
                        testSizeField.setText(result.getResponseBytes() + " bytes");
                        testResponseArea.setText(result.getPrettyJson());
                    });
                }
            } catch (Exception ex) {
                logPanel.log("ERROR", "测试失败：" + ex.getMessage());
                SwingUtilities.invokeLater(() -> testStatusField.setText("失败"));
            }
        });
    }

    private TaskScheduler ensureScheduler(TaskDefinition task) {
        return schedulers.computeIfAbsent(task.getTaskId(), key -> new TaskScheduler(task, cancelled -> createJob(task, cancelled),
                ensureLogPanel(task)));
    }

    private TaskScheduler.TaskExecutable createJob(TaskDefinition task, AtomicBoolean cancelled) {
        Timeout connectTimeout = Timeout.ofSeconds(10);
        Timeout responseTimeout = Timeout.ofSeconds(30);
        HttpClientFactory factory = new HttpClientFactory(config.isHttpsInsecure(), connectTimeout, responseTimeout);
        CloseableHttpClient client = factory.createClient();
        SourceClient sourceClient = new SourceClient(client, connectTimeout, responseTimeout);
        AsyncSqlClient asyncSqlClient = new AsyncSqlClient(client, connectTimeout, responseTimeout);
        TaskSqlBuilder sqlBuilder = new TaskSqlBuilder();
        UiLogPanel logPanel = ensureLogPanel(task);
        TaskStatusPanel statusPanel = ensureStatusPanel(task);
        TaskTransferJob job = new TaskTransferJob(task, config, sourceClient, asyncSqlClient, sqlBuilder,
                logPanel, statusPanel, cancelled);
        return () -> {
            try {
                return job.run();
            } finally {
                try {
                    client.close();
                } catch (Exception ignored) {
                    // ignore close failures
                }
            }
        };
    }

    private UiLogPanel ensureLogPanel(TaskDefinition task) {
        return logPanels.computeIfAbsent(task.getTaskId(), ignored -> new UiLogPanel());
    }

    private TaskStatusPanel ensureStatusPanel(TaskDefinition task) {
        return statusPanels.computeIfAbsent(task.getTaskId(), ignored -> new TaskStatusPanel());
    }

    private UiLogSink uiLogForApp() {
        UiLogPanel panel = logPanels.computeIfAbsent("_app", ignored -> new UiLogPanel());
        return panel;
    }
}
