package com.transdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.util.Timeout;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.GridLayout;
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
    private static final Insets FORM_INSETS = new Insets(8, 8, 8, 8);
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
    private final JTextArea mappingPreviewArea = new JTextArea(6, 32);
    private final JTextArea previewSqlArea = new JTextArea(6, 32);
    private final JTextField conflictTargetField = new JTextField(20);
    private final JTextField scopeKeyField = new JTextField(24);
    private final JTextField naturalKeyField = new JTextField(24);
    private final JTextField distinctKeyField = new JTextField(24);

    private final JTextArea testResponseArea = new JTextArea();
    private final JTextField testStatusField = new JTextField(20);
    private final JTextField testElapsedField = new JTextField(20);
    private final JTextField testSizeField = new JTextField(20);
    private final JCheckBox uniqueConstraintCheck = new JCheckBox("建议唯一约束/索引");
    private final JButton autoGenerateButton = new JButton("自动生成DDL与模板");
    private final ObjectMapper objectMapper = new ObjectMapper();
    private List<JsonNode> lastTestRecords = List.of();

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
        splitPane.setResizeWeight(0.25);
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
        panel.setMinimumSize(new Dimension(240, 200));
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
        JPanel toolBar = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
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

        JPanel toolbar = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
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
        JPanel logsTab = buildLogsPanel();
        tabs.addTab("日志", logsTab);

        panel.add(toolbar, BorderLayout.NORTH);
        panel.add(tabs, BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildBasicPanel() {
        JPanel content = new JPanel(new GridBagLayout());

        taskIdField.setEditable(false);

        addFormRow(content, 0, "任务ID", taskIdField);
        addFormRow(content, 1, "任务名称", taskNameField);
        addFormRow(content, 2, "启用状态", enabledCheck);
        addFormRow(content, 3, "源 POST URL", sourceUrlField);
        addFormRow(content, 4, "请求体", new JScrollPane(sourceBodyArea), GridBagConstraints.BOTH, 0.3, true);
        addFormRow(content, 5, "分段大小", batchSizeSpinner);
        addFillerRow(content, 6);

        return wrapScrollableTab(content);
    }

    private JPanel buildSchedulePanel() {
        JPanel content = new JPanel(new GridBagLayout());
        addFormRow(content, 0, "间隔（秒）", intervalValue);
        addFormRow(content, 1, "窗口开始", windowStartField);
        addFormRow(content, 2, "窗口结束", windowEndField);
        addFormRow(content, 3, "最大重试", maxRetriesSpinner);
        addFormRow(content, 4, "锁租约秒数", leaseSecondsSpinner);
        addFillerRow(content, 5);

        return wrapScrollableTab(content);
    }

    private JPanel buildTargetPanel() {
        JPanel content = new JPanel(new GridBagLayout());
        mappingPreviewArea.setEditable(false);
        mappingPreviewArea.setLineWrap(false);
        previewSqlArea.setEditable(false);
        previewSqlArea.setLineWrap(false);

        addFormRow(content, 0, "目标 Schema", targetSchemaField);
        addFormRow(content, 1, "目标表", targetTableField);
        addFormRow(content, 2, "建表 SQL", new JScrollPane(createTableArea), GridBagConstraints.BOTH, 0.3, true);
        addFormRow(content, 3, "插入模式", insertModeCombo);
        addFormRow(content, 4, "插入模板", new JScrollPane(insertTemplateArea), GridBagConstraints.BOTH, 0.3, true);
        addFormRow(content, 5, "冲突目标", conflictTargetField);
        addFormRow(content, 6, "范围键字段", scopeKeyField);
        addFormRow(content, 7, "自然键字段", naturalKeyField);
        addFormRow(content, 8, "去重字段", distinctKeyField);
        addFormRow(content, 9, "映射预览", new JScrollPane(mappingPreviewArea), GridBagConstraints.BOTH, 0.3, true);
        addFormRow(content, 10, "插入预览SQL", new JScrollPane(previewSqlArea), GridBagConstraints.BOTH, 0.3, true);
        addFillerRow(content, 11);

        return wrapScrollableTab(content);
    }

    private JPanel buildTestPanel() {
        JPanel content = new JPanel(new GridBagLayout());
        JPanel infoPanel = new JPanel(new GridBagLayout());
        addFormRow(infoPanel, 0, "HTTP 状态", testStatusField);
        addFormRow(infoPanel, 1, "耗时", testElapsedField);
        addFormRow(infoPanel, 2, "响应大小", testSizeField);
        testStatusField.setEditable(false);
        testElapsedField.setEditable(false);
        testSizeField.setEditable(false);

        JButton testButton = new JButton("测试源接口");
        testButton.addActionListener(event -> runTest());
        autoGenerateButton.setEnabled(false);
        autoGenerateButton.addActionListener(event -> autoGenerateFromTest());

        JPanel actionPanel = new JPanel(new GridLayout(3, 1, 4, 4));
        actionPanel.add(testButton);
        actionPanel.add(autoGenerateButton);
        actionPanel.add(uniqueConstraintCheck);

        JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(infoPanel, BorderLayout.CENTER);
        topPanel.add(actionPanel, BorderLayout.EAST);

        testResponseArea.setEditable(false);
        testResponseArea.setLineWrap(false);
        JScrollPane scrollPane = new JScrollPane(testResponseArea);

        GridBagConstraints topGbc = new GridBagConstraints();
        topGbc.gridx = 0;
        topGbc.gridy = 0;
        topGbc.insets = FORM_INSETS;
        topGbc.anchor = GridBagConstraints.NORTHWEST;
        topGbc.fill = GridBagConstraints.HORIZONTAL;
        topGbc.weightx = 1;
        content.add(topPanel, topGbc);

        GridBagConstraints responseGbc = new GridBagConstraints();
        responseGbc.gridx = 0;
        responseGbc.gridy = 1;
        responseGbc.insets = FORM_INSETS;
        responseGbc.anchor = GridBagConstraints.NORTHWEST;
        responseGbc.fill = GridBagConstraints.BOTH;
        responseGbc.weightx = 1;
        responseGbc.weighty = 0.6;
        content.add(scrollPane, responseGbc);

        GridBagConstraints fillerGbc = new GridBagConstraints();
        fillerGbc.gridx = 0;
        fillerGbc.gridy = 2;
        fillerGbc.weightx = 1;
        fillerGbc.weighty = 1;
        fillerGbc.fill = GridBagConstraints.BOTH;
        content.add(Box.createGlue(), fillerGbc);

        return wrapScrollableTab(content);
    }

    private JPanel buildLogsPanel() {
        JPanel content = new JPanel(new BorderLayout());
        JPanel wrapper = new JPanel(new BorderLayout());
        JScrollPane scrollPane = new JScrollPane(content);
        scrollPane.getVerticalScrollBar().setUnitIncrement(16);
        wrapper.add(scrollPane, BorderLayout.CENTER);
        logsContainer = content;
        return wrapper;
    }

    private JPanel wrapScrollableTab(JPanel content) {
        JPanel panel = new JPanel(new BorderLayout());
        JScrollPane scrollPane = new JScrollPane(content);
        scrollPane.getVerticalScrollBar().setUnitIncrement(16);
        panel.add(scrollPane, BorderLayout.CENTER);
        return panel;
    }

    private void addFormRow(JPanel panel, int row, String labelText, java.awt.Component field) {
        addFormRow(panel, row, labelText, field, GridBagConstraints.HORIZONTAL, 0, false);
    }

    private void addFormRow(JPanel panel, int row, String labelText, java.awt.Component field, int fill, double weighty,
                            boolean labelTopAlign) {
        GridBagConstraints labelGbc = new GridBagConstraints();
        labelGbc.gridx = 0;
        labelGbc.gridy = row;
        labelGbc.insets = FORM_INSETS;
        labelGbc.anchor = labelTopAlign ? GridBagConstraints.NORTHEAST : GridBagConstraints.EAST;
        labelGbc.fill = GridBagConstraints.NONE;
        labelGbc.weightx = 0;
        panel.add(new JLabel(labelText), labelGbc);

        GridBagConstraints fieldGbc = new GridBagConstraints();
        fieldGbc.gridx = 1;
        fieldGbc.gridy = row;
        fieldGbc.insets = FORM_INSETS;
        fieldGbc.anchor = GridBagConstraints.NORTHWEST;
        fieldGbc.fill = fill;
        fieldGbc.weightx = 1;
        fieldGbc.weighty = weighty;
        panel.add(field, fieldGbc);
    }

    private void addFillerRow(JPanel panel, int row) {
        GridBagConstraints fillerGbc = new GridBagConstraints();
        fillerGbc.gridx = 0;
        fillerGbc.gridy = row;
        fillerGbc.gridwidth = 2;
        fillerGbc.weightx = 1;
        fillerGbc.weighty = 1;
        fillerGbc.fill = GridBagConstraints.BOTH;
        panel.add(Box.createGlue(), fillerGbc);
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
        lastTestRecords = List.of();
        autoGenerateButton.setEnabled(false);
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
                mappingPreviewArea.setText("");
                previewSqlArea.setText("");
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
            mappingPreviewArea.setText("");
            previewSqlArea.setText("");
        } finally {
            updatingFields = false;
        }
    }

    private void applyChanges() {
        if (updatingFields || selectedTask == null) {
            return;
        }
        selectedTask.setTaskName(taskNameField.getText().trim());
        boolean enableRequested = enabledCheck.isSelected();
        String insertTemplateText = insertTemplateArea.getText();
        if (enableRequested && (insertTemplateText == null || insertTemplateText.isBlank())) {
            JOptionPane.showMessageDialog(this, "插入模板不能为空，任务无法启用。请先生成或填写插入模板。",
                    "校验提示", JOptionPane.WARNING_MESSAGE);
            updatingFields = true;
            try {
                enabledCheck.setSelected(false);
            } finally {
                updatingFields = false;
            }
            enableRequested = false;
        }
        selectedTask.setEnabled(enableRequested);
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
        target.setInsertTemplate(insertTemplateText);
        target.setConflictTarget(conflictTargetField.getText().trim());
        target.getAuditSettings().setScopeKeyFields(splitFields(scopeKeyField.getText()));
        target.getAuditSettings().setNaturalKeyFields(splitFields(naturalKeyField.getText()));
        target.getAuditSettings().setDistinctKeyJsonField(distinctKeyField.getText().trim());

        saveTasks();
        taskList.repaint();
        if (target.getInsertMode() == InsertMode.SKIP_DUPLICATES
                && (target.getConflictTarget() == null || target.getConflictTarget().isBlank())) {
            ensureLogPanel(selectedTask).log("WARN", "SKIP_DUPLICATES 未配置冲突目标或唯一约束，可能导致运行时冲突处理无效。");
        }
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
        autoGenerateButton.setEnabled(false);
        lastTestRecords = List.of();
        backgroundExecutor.submit(() -> {
            try {
                Timeout connectTimeout = Timeout.ofSeconds(10);
                Timeout responseTimeout = Timeout.ofSeconds(30);
                HttpClientFactory factory = new HttpClientFactory(config.isHttpsInsecure(), connectTimeout, responseTimeout);
                try (CloseableHttpClient client = factory.createClient()) {
                    SourceClient sourceClient = new SourceClient(client, connectTimeout, responseTimeout);
                    SourceFetchResult result = sourceClient.testPost(selectedTask, config);
                    List<JsonNode> records = List.of();
                    boolean parsed = false;
                    try {
                        JsonNode root = objectMapper.readTree(result.getRawJson());
                        records = extractRecordsFromTestResponse(root);
                        parsed = true;
                    } catch (Exception ignored) {
                        // keep unparsed
                    }
                    List<JsonNode> finalRecords = records;
                    boolean finalParsed = parsed;
                    SwingUtilities.invokeLater(() -> {
                        testStatusField.setText(String.valueOf(result.getStatusCode()));
                        testElapsedField.setText(result.getElapsedMillis() + " ms");
                        testSizeField.setText(result.getResponseBytes() + " bytes");
                        testResponseArea.setText(result.getPrettyJson());
                        lastTestRecords = finalRecords;
                        autoGenerateButton.setEnabled(finalParsed);
                        if (!finalParsed) {
                            logPanel.log("WARN", "响应非标准 JSON，无法自动生成 DDL 与插入模板。");
                        } else if (finalRecords.isEmpty()) {
                            logPanel.log("WARN", "No data array records detected; generated generic payload structure; please edit manually.");
                        }
                    });
                }
            } catch (Exception ex) {
                logPanel.log("ERROR", "测试失败：" + ex.getMessage());
                SwingUtilities.invokeLater(() -> testStatusField.setText("失败"));
            }
        });
    }

    private void autoGenerateFromTest() {
        if (selectedTask == null) {
            return;
        }
        UiLogPanel logPanel = ensureLogPanel(selectedTask);
        if (lastTestRecords == null) {
            JOptionPane.showMessageDialog(this, "暂无可用的测试样本，请先测试源接口。", "提示",
                    JOptionPane.WARNING_MESSAGE);
            return;
        }
        boolean suggestUnique = uniqueConstraintCheck.isSelected();
        backgroundExecutor.submit(() -> {
            AutoSqlGenerator generator = new AutoSqlGenerator();
            AutoSqlGenerator.AutoSqlResult result = generator.generate(selectedTask, lastTestRecords, suggestUnique);
            SwingUtilities.invokeLater(() -> {
                updatingFields = true;
                try {
                    targetSchemaField.setText(result.targetSchema());
                    targetTableField.setText(result.targetTable());
                    createTableArea.setText(result.createTableSql());
                    insertTemplateArea.setText(result.insertTemplate());
                    mappingPreviewArea.setText(result.mappingPreview());
                    previewSqlArea.setText(result.previewSql());
                } finally {
                    updatingFields = false;
                }
                applyChanges();
                if (!result.warnings().isEmpty()) {
                    for (String warning : result.warnings()) {
                        logPanel.log("WARN", warning);
                    }
                } else {
                    logPanel.log("INFO", "已生成建表 SQL 与插入模板，可在保存前调整。");
                }
            });
        });
    }

    private List<JsonNode> extractRecordsFromTestResponse(JsonNode root) {
        if (root == null || root.isNull() || !root.isObject()) {
            return List.of();
        }
        JsonNode data = root.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) {
            return List.of();
        }
        JsonNode first = data.get(0);
        if (first == null || first.isNull() || !first.isObject()) {
            return List.of();
        }
        List<JsonNode> records = new ArrayList<>();
        for (JsonNode item : data) {
            if (item != null && item.isObject()) {
                records.add(item);
            }
        }
        return records;
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
