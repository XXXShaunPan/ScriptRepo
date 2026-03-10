"""PyQt application entry."""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

import requests
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtGui import QPixmap
from PyQt5.QtWidgets import (
    QApplication,
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QFormLayout,
    QGridLayout,
    QLineEdit,
    QLabel,
    QHBoxLayout,
    QMessageBox,
    QMainWindow,
    QProgressDialog,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTextBrowser,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from quick_matching_tool.config.logging import setup_logging
from quick_matching_tool.config.settings import CURRENT_VERSION, ENABLE_REMOTE_CODE, VERSION_CHECK_URL
from quick_matching_tool.utils.tools import build_gsheet_url, extract_gsheet_info
from quick_matching_tool.ui.logging import LogHandler
from quick_matching_tool.ui.threads import (
    BatchAddThread,
    CodeSyncThread,
    GSheetLoadThread,
    ToolThread,
    UpdateCheckThread,
    UpdateDownloadThread,
)


class ManualBatchDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("添加批次号")
        self.setMinimumWidth(520)

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.batch_name_input = QLineEdit()
        self.batch_name_input.setPlaceholderText("填入批次号名称，用作标签")
        self.batch_name_input.setFixedWidth(500)
        form.addRow("批次号名称", self.batch_name_input)

        self.gsheet_url_input = QLineEdit()
        self.gsheet_url_input.setPlaceholderText("填入Google Sheet的链接")
        self.gsheet_url_input.setFixedWidth(500)
        form.addRow("GSheet 链接", self.gsheet_url_input)

        self.pic_url_input = QLineEdit()
        self.pic_url_input.setPlaceholderText("填入复制过来的public share link")
        self.pic_url_input.setFixedWidth(500)
        form.addRow("Public 链接", self.pic_url_input)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok
                                      | QDialogButtonBox.Cancel)
        ok_button = button_box.button(QDialogButtonBox.Ok)
        cancel_button = button_box.button(QDialogButtonBox.Cancel)
        ok_button.setText("添加")
        cancel_button.setText("取消")
        button_box.setStyleSheet("""
            QPushButton {
                min-width: 96px;
                border-radius: 10px;
                padding: 8px 14px;
                font-weight: 700;
                border: 1px solid #d1d5db;
            }
            QPushButton:hover {
                border: 1px solid #9ca3af;
            }
            QPushButton:pressed {
                border: 1px solid #6b7280;
            }
        """)
        ok_button.setStyleSheet("""
            QPushButton {
                background-color: #2563eb;
                color: #ffffff;
                border: 1px solid #1d4ed8;
            }
            QPushButton:hover {
                background-color: #1d4ed8;
            }
            QPushButton:pressed {
                background-color: #1e40af;
            }
        """)
        cancel_button.setStyleSheet("""
            QPushButton {
                background-color: #e5e7eb;
                color: #111827;
            }
            QPushButton:hover {
                background-color: #d1d5db;
            }
            QPushButton:pressed {
                background-color: #9ca3af;
            }
        """)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def values(self):
        return {
            "gsheet_name": self.batch_name_input.text().strip(),
            "data_url": self.gsheet_url_input.text().strip(),
            "pic_url": self.pic_url_input.text().strip(),
        }


class ClickableImageLabel(QLabel):
    clicked = pyqtSignal()

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)


class TutorialImagePreviewDialog(QDialog):
    def __init__(self, title: str, pixmap: QPixmap, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(980, 760)

        layout = QVBoxLayout(self)
        image_label = QLabel()
        image_label.setAlignment(Qt.AlignCenter)
        image_label.setStyleSheet(
            "background-color: #111827; border-radius: 8px;")
        image_label.setPixmap(
            pixmap.scaled(940, 680, Qt.KeepAspectRatio,
                          Qt.SmoothTransformation))
        layout.addWidget(image_label)

        button_box = QDialogButtonBox(QDialogButtonBox.Close)
        close_button = button_box.button(QDialogButtonBox.Close)
        close_button.setText("关闭")
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)


class TutorialDialog(QDialog):
    def __init__(self,
                 title: str,
                 intro_html: str,
                 image_cards: list,
                 missing_files: list,
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(960, 820)

        layout = QVBoxLayout(self)

        intro_browser = QTextBrowser()
        intro_browser.setHtml(intro_html)
        intro_browser.setReadOnly(True)
        intro_browser.setOpenExternalLinks(True)
        intro_browser.setFrameShape(QFrame.NoFrame)
        intro_browser.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        intro_browser.setMaximumHeight(300)
        intro_browser.setTextInteractionFlags(Qt.TextBrowserInteraction
                                              | Qt.TextSelectableByMouse
                                              | Qt.TextSelectableByKeyboard)
        intro_browser.setStyleSheet("""
            QTextBrowser {
                color: #1f2937;
                background: transparent;
                padding: 2px 0 0 0;
            }
        """)
        layout.addWidget(intro_browser)

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setStyleSheet("border: none;")
        cards_widget = QWidget()
        cards_layout = QGridLayout(cards_widget)
        cards_layout.setContentsMargins(0, 2, 0, 2)
        cards_layout.setHorizontalSpacing(12)
        cards_layout.setVerticalSpacing(10)

        for idx, card in enumerate(image_cards):
            card_widget = self._build_card(card["title"], card["pixmap"])
            cards_layout.addWidget(card_widget, idx // 2, idx % 2)

        if missing_files:
            missing = QLabel(
                "以下教程图片加载失败：<br>" +
                "<br>".join([f"- {item}" for item in missing_files]))
            missing.setTextFormat(Qt.RichText)
            missing.setWordWrap(True)
            missing.setStyleSheet(
                "color: #b45309; background: #fffbeb; border: 1px solid #fde68a; border-radius: 8px; padding: 8px;"
            )
            cards_layout.addWidget(missing, (len(image_cards) + 1) // 2 + 1, 0,
                                   1, 2)

        scroll_area.setWidget(cards_widget)
        layout.addWidget(scroll_area)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok)
        ok_button = button_box.button(QDialogButtonBox.Ok)
        ok_button.setText("无人在意")
        button_box.setStyleSheet("""
            QPushButton {
                min-width: 120px;
                border-radius: 10px;
                padding: 8px 16px;
                font-weight: 700;
                background-color: #2563eb;
                color: #ffffff;
                border: 1px solid #1d4ed8;
            }
            QPushButton:hover {
                background-color: #1d4ed8;
            }
            QPushButton:pressed {
                background-color: #1e40af;
            }
        """)
        button_box.accepted.connect(self.accept)
        layout.addWidget(button_box)

    def _build_card(self, title: str, pixmap: QPixmap) -> QWidget:
        card = QFrame()
        card.setStyleSheet("""
            QFrame {
                background: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 10px;
            }
        """)
        vbox = QVBoxLayout(card)
        vbox.setContentsMargins(10, 10, 10, 10)
        vbox.setSpacing(6)

        title_label = QLabel(title)
        title_label.setWordWrap(True)
        title_label.setStyleSheet(
            "font-size: 13px; font-weight: 600; color: #111827;")
        vbox.addWidget(title_label)

        image_label = ClickableImageLabel()
        image_label.setAlignment(Qt.AlignCenter)
        image_label.setStyleSheet("""
            QLabel {
                background: #f8fafc;
                border: 1px solid #d1d5db;
                border-radius: 8px;
                padding: 2px;
            }
            QLabel:hover {
                border: 1px solid #2563eb;
            }
        """)
        image_label.setPixmap(
            pixmap.scaled(360, 220, Qt.KeepAspectRatio,
                          Qt.SmoothTransformation))
        image_label.clicked.connect(
            lambda p=pixmap, t=title: TutorialImagePreviewDialog(t, p, self
                                                                 ).exec_())
        vbox.addWidget(image_label)

        tip_label = QLabel("点击图片放大")
        tip_label.setAlignment(Qt.AlignRight)
        tip_label.setStyleSheet("font-size: 12px; color: #6b7280;")
        vbox.addWidget(tip_label)
        return card


class QuickMatchingApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Quick Matching Tool")
        screen_geometry = QApplication.desktop().availableGeometry()
        center_x = int((screen_geometry.width() - 1200) / 2)
        center_y = int((screen_geometry.height() - 1000) / 2)
        self.move(center_x, center_y)
        self.setGeometry(center_x, center_y, 1200, 1200)

        self.gsheet_url_info = []
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        self.apply_styles()

        splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(splitter)

        top_widget = QWidget()
        layout = QVBoxLayout(top_widget)

        usage_area = QTextEdit()
        usage_area.setReadOnly(True)
        usage_area.setMaximumHeight(300)
        usage_html = """
        <p style="font-size: 14pt; font-weight: bold;">使用说明：</p>
        <p style="margin-left: 20px;">
            1. 选择需要执行的批次号；<br>
            2. 点击"运行"按钮开始执行；<br>
            <span style="color: red; font-weight: bold;">3. 习惯性每次点击同步代码按钮 is a good habit.</span><br>
        </p>
        <p style="font-size: 14pt; font-weight: bold;">注意事项：</p>
        <p style="margin-left: 20px;">
            <span style="color: red; font-size: 14pt; font-weight: bold;">1. 运行过程中请勿关闭浏览器窗口，否则可能会导致运行失败；</span><br>
            2. 日志区域会显示详细的执行信息。
        </p>
        """
        usage_area.setHtml(usage_html)

        layout.addWidget(QLabel("使用说明："))
        layout.addWidget(usage_area)

        layout.addWidget(QLabel("选择批次号:"))
        self.batch_no_layout = QHBoxLayout()
        self.batch_no_checkboxes = []
        self.batch_no_loading_label = QLabel("正在加载批次号...")
        self.batch_no_progress = QProgressBar()
        self.batch_no_progress.setRange(0, 0)
        self.batch_no_progress.setTextVisible(False)
        self.batch_no_layout.addWidget(self.batch_no_loading_label)
        self.batch_no_layout.addWidget(self.batch_no_progress)

        self.batch_no_widget = QWidget()
        self.batch_no_widget.setLayout(self.batch_no_layout)
        layout.addWidget(self.batch_no_widget)

        button_layout = QHBoxLayout()
        self.run_button = QPushButton("运行")
        self.run_button.setObjectName("run_button")
        self.run_button.clicked.connect(self.run_tool)
        button_layout.addWidget(self.run_button)

        self.stop_button = QPushButton("中断")
        self.stop_button.setObjectName("stop_button")
        self.stop_button.clicked.connect(self.stop_tool)
        self.stop_button.setEnabled(False)
        button_layout.addWidget(self.stop_button)

        self.manual_batch_button = QPushButton("手动添加批次号")
        self.manual_batch_button.setObjectName("manual_batch_button")
        self.manual_batch_button.clicked.connect(self.add_manual_batch)
        button_layout.addWidget(self.manual_batch_button)

        self.update_button = QPushButton(f"检查更新 (当前版本: v{CURRENT_VERSION})")
        self.update_button.setObjectName("update_button")
        self.update_button.clicked.connect(self.check_for_updates)
        button_layout.addWidget(self.update_button)

        if ENABLE_REMOTE_CODE:
            self.sync_code_button = QPushButton("同步代码")
            self.sync_code_button.setObjectName("sync_code_button")
            self.sync_code_button.clicked.connect(self.sync_remote_code)
            button_layout.addWidget(self.sync_code_button)

        button_widget = QWidget()
        button_widget.setLayout(button_layout)
        layout.addWidget(button_widget)

        splitter.addWidget(top_widget)

        log_widget = QWidget()
        log_layout = QVBoxLayout(log_widget)
        log_layout.addWidget(QLabel("输出日志："))

        self.log_area = QTextEdit()
        self.log_area.setObjectName("log_area")
        self.log_area.setReadOnly(True)
        self.log_area.setLineWrapMode(QTextEdit.NoWrap)
        log_layout.addWidget(self.log_area)

        splitter.addWidget(log_widget)
        splitter.setSizes([200, 300])

        self.log_handler = LogHandler()
        self.log_handler.new_log.connect(self.append_log)
        setup_logging(handler=self.log_handler)

        self.start_load_gsheet_url_info()

    def apply_styles(self):
        self.setStyleSheet("""
            QMainWindow, QWidget {
                background-color: #f5f7fb;
                color: #1f2937;
                font-family: "Helvetica Neue", Arial;
                font-size: 13px;
            }
            QLabel {
                color: #111827;
            }
            QTextEdit {
                background-color: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 10px;
                padding: 10px;
            }
            QTextEdit#log_area {
                background-color: #0b1220;
                border: 1px solid #111827;
                border-radius: 12px;
                padding: 12px;
                color: #e5e7eb;
                font-family: "Menlo", "Monaco", "Courier New";
                font-size: 12px;
            }
            QCheckBox {
                spacing: 8px;
                padding: 6px 4px;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
                border-radius: 4px;
                border: 1px solid #cbd5e1;
                background: #ffffff;
            }
            QCheckBox::indicator:checked {
                background: #2563eb;
                border: 1px solid #2563eb;
            }
            QPushButton {
                border: none;
                border-radius: 16px;
                padding: 8px 18px;
                color: #ffffff;
                font-weight: 600;
            }
            QPushButton#run_button {
                background-color: #10b981;
            }
            QPushButton#run_button:hover {
                background-color: #0ea371;
            }
            QPushButton#run_button:disabled {
                background-color: #9ca3af;
                color: #f3f4f6;
                border: 1px solid #94a3b8;
                opacity: 0.7;
            }
            QPushButton#stop_button {
                background-color: #ef4444;
            }
            QPushButton#stop_button:hover {
                background-color: #dc2626;
            }
            QPushButton#stop_button:disabled {
                background-color: #9ca3af;
                color: #f3f4f6;
                border: 1px solid #94a3b8;
                opacity: 0.7;
            }
            QPushButton#update_button {
                background-color: #3b82f6;
            }
            QPushButton#update_button:hover {
                background-color: #2563eb;
            }
            QPushButton#update_button:disabled {
                background-color: #9ca3af;
                color: #f3f4f6;
                border: 1px solid #94a3b8;
                opacity: 0.7;
            }
            QPushButton#manual_batch_button {
                background-color: #14b8a6;
            }
            QPushButton#manual_batch_button:hover {
                background-color: #0d9488;
            }
            QPushButton#manual_batch_button:disabled {
                background-color: #9ca3af;
                color: #f3f4f6;
                border: 1px solid #94a3b8;
                opacity: 0.7;
            }
            QPushButton#sync_code_button {
                background-color: #8b5cf6;
            }
            QPushButton#sync_code_button:hover {
                background-color: #7c3aed;
            }
            QPushButton#sync_code_button:disabled {
                background-color: #9ca3af;
                color: #f3f4f6;
                border: 1px solid #94a3b8;
                opacity: 0.7;
            }
            QMessageBox QPushButton {
                background-color: #e5e7eb;
                color: #111827;
                border: 1px solid #d1d5db;
                border-radius: 10px;
                padding: 6px 16px;
                min-width: 84px;
                font-weight: 600;
            }
            QMessageBox QPushButton:hover {
                background-color: #dbe2ea;
            }
            QMessageBox QPushButton:pressed {
                background-color: #cbd5e1;
            }
            QPushButton:disabled {
                background-color: #9ca3af;
                color: #f3f4f6;
                border: 1px solid #94a3b8;
                opacity: 0.7;
            }
            QProgressBar {
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                background-color: #ffffff;
                height: 10px;
            }
            QProgressBar::chunk {
                border-radius: 8px;
                background-color: #3b82f6;
            }
            QSplitter::handle {
                background-color: #e5e7eb;
            }
        """)

    def start_load_gsheet_url_info(self):
        self.run_button.setEnabled(False)
        self.batch_no_loading_label.setText("正在加载批次号...")
        self.batch_no_progress.show()
        self.load_thread = GSheetLoadThread()
        self.load_thread.load_finished.connect(self.on_gsheet_loaded)
        self.load_thread.start()

    def on_gsheet_loaded(self, success: bool, message: str, data: list):
        self.batch_no_progress.hide()
        if success:
            self.gsheet_url_info = data
            self.refresh_batch_no_layout()
            self.run_button.setEnabled(True)
        else:
            self.batch_no_loading_label.setText("批次号加载失败")
            QMessageBox.warning(self, "提示", message)

    def refresh_batch_no_layout(self):
        while self.batch_no_layout.count():
            item = self.batch_no_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

        self.batch_no_checkboxes = []
        all_batch_nos = [info["gsheet_name"] for info in self.gsheet_url_info]
        for batch_no in all_batch_nos:
            checkbox = QCheckBox(batch_no)
            self.batch_no_checkboxes.append(checkbox)
            self.batch_no_layout.addWidget(checkbox)

        if not self.batch_no_checkboxes:
            empty_label = QLabel("未加载到批次号")
            self.batch_no_layout.addWidget(empty_label)

    def append_log(self, message, level):
        color_map = {
            logging.DEBUG: "#FFFF00",
            logging.INFO: "#5eba5e",
            logging.WARNING: "#FFA500",
            logging.ERROR: "#FF0000",
            logging.CRITICAL: "#8B0000",
        }
        color = color_map.get(level, "#5eba5e")
        colored_message = (
            f'<div style="color: {color}; padding: 4px 0; '
            f'border-bottom: 1px solid rgba(148, 163, 184, 0.35); '
            f'line-height: 1.35;">{message}</div>')
        self.log_area.append(colored_message)
        self.log_area.verticalScrollBar().setValue(
            self.log_area.verticalScrollBar().maximum())

    def get_selected_batch_nos(self):
        selected_batch_nos = []
        for checkbox in self.batch_no_checkboxes:
            if checkbox.isChecked():
                selected_batch_nos.append(checkbox.text())
        if selected_batch_nos:
            return selected_batch_nos
        if self.batch_no_checkboxes:
            return [self.batch_no_checkboxes[0].text()]
        return []

    def add_manual_batch(self):
        self.show_manual_batch_tutorial()

        dialog = ManualBatchDialog(self)
        if dialog.exec_() != QDialog.Accepted:
            return

        values = dialog.values()
        gsheet_url = values["data_url"]
        pic_url = values["pic_url"]
        if not gsheet_url.startswith("http") or not '/d/e/' in pic_url:
            QMessageBox.warning(self, "输入有误", "请正确填写有效的 GSheet/Public_Share链接")
            return

        batch_name = values["gsheet_name"]
        if any(
                info.get("gsheet_name") == batch_name
                for info in self.gsheet_url_info):
            QMessageBox.warning(self, "添加失败", f"批次号 {batch_name} 已存在，请更换名称")
            return
        pic_url = build_gsheet_url(*extract_gsheet_info(pic_url))
        gsheet_id = extract_gsheet_info(gsheet_url)[1]
        self.gsheet_url_info.append({
            "gsheet_name": batch_name,
            "data_url": gsheet_id,
            "pic_url": pic_url,
        })
        self.batch_add_thread = BatchAddThread(
            batch_name, gsheet_id, pic_url,
            self.gsheet_url_info.__len__() + 1)
        self.run_button.setEnabled(False)
        self.batch_add_thread.add_finished.connect(lambda success, message: (
            QMessageBox.information(self, "添加成功", f"已添加批次号：{batch_name}")
            if success else QMessageBox.warning(self, "添加失败", message),
            self.refresh_batch_no_layout(), self.run_button.setEnabled(True)))
        self.batch_add_thread.start()

    def show_manual_batch_tutorial(self):
        intro_html, image_cards, missing_files = self.build_manual_batch_tutorial_data(
        )
        tutorial_dialog = TutorialDialog("操作教程", intro_html, image_cards,
                                         missing_files, self)
        tutorial_dialog.exec_()

    def build_manual_batch_tutorial_data(self) -> tuple[str, list, list]:
        # image_dir = Path(
        #     __file__).resolve().parents[3] / "docs" / "manual_batch_images"
        image_items = [
            ("步骤 1：打开Google Sheet里的Public Share入口",
             "https://picui.ogmua.cn/s1/2026/03/06/69aa4442860b1.webp"),
            ("步骤 2：选择评分表后点击分享",
             "https://picui.ogmua.cn/s1/2026/03/06/69aa4442d5538.webp"),
            ("步骤 3：复制Public Share链接",
             "https://picui.ogmua.cn/s1/2026/03/06/69aa44461b6b8.webp"),
        ]

        image_cards = []
        missing_files = []
        for title, image_ref in image_items:
            pixmap, err = self._resolve_tutorial_image_pixmap(image_ref)
            if pixmap is None:
                missing_files.append(f"{title} ({image_ref}) - {err}")
                continue
            image_cards.append({"title": title, "pixmap": pixmap})

        intro_html = """
        <h2 style="margin: 0 0 6px 0; font-size: 18px;">添加批次号操作教程</h2>
        <h3 style="margin: 0 0 6px 0; font-size: 16px;"><b>重要：</b>确保每个批次表都有以下机器人邮箱的edit权限</h3>
        <ul style="margin: 0 0 8px 0; padding: 0; line-height: 1.4; list-style-type: none;">
            <li><b>googlesheets@ins-kol.iam.gserviceaccount.com</b></li>
            <li><b>p1-332@project1-350802.iam.gserviceaccount.com</b></li>
            <li><b>project2-service-account1@project2-351001.iam.gserviceaccount.com</b></li>
            <li><b>scsbibot01@elite-truck-420707.iam.gserviceaccount.com</b></li>
            <li><b>rogerbot@roger-project-420707.iam.gserviceaccount.com</b></li>
            <li>joycebot@joyce-project-420708.iam.gserviceaccount.com</li>
            <li><b>aaronbot@aaron-project-420708.iam.gserviceaccount.com</b></li>
            <li><b>wendy-xu-shopee-com@perfect-science-290507.iam.gserviceaccount.com</b></li>
            <li><b>shaunbot2@clever-mantra-446411-u2.iam.gserviceaccount.com</b></li>
            <li><b>lovito-bi@lovito.iam.gserviceaccount.com</b></li>
        </ul>
        <p style="margin: 0 0 6px 0; font-size: 14px;"><b>可全选以上邮箱复制到Gsheet分享</b></p>
        """
        return intro_html, image_cards, missing_files

    def _resolve_tutorial_image_pixmap(
            self, image_ref: str) -> tuple[QPixmap | None, str]:
        if image_ref.startswith(("http://", "https://")):
            try:
                response = requests.get(image_ref, timeout=20)
                response.raise_for_status()
                pixmap = QPixmap()
                if not pixmap.loadFromData(response.content):
                    return None, "图片格式无法识别"
                return pixmap, ""
            except Exception as exc:
                logging.warning("教程图片下载失败: %s, err=%s", image_ref, exc)
                return None, f"下载失败: {exc}"

        path = Path(image_ref)
        if path.exists():
            pixmap = QPixmap(str(path))
            if pixmap.isNull():
                return None, "本地图片格式无法识别"
            return pixmap, ""
        return None, "本地文件不存在"

    def run_tool(self):
        batch_nos = self.get_selected_batch_nos()
        if not batch_nos:
            QMessageBox.critical(self, "错误", "请至少选择一个批次号")
            return

        self.log_area.clear()
        logging.info("开始运行 Quick Matching Tool，批次号: %s", ", ".join(batch_nos))

        self.run_button.setEnabled(False)
        self.stop_button.setEnabled(True)

        source_gsheet_url_info = list(
            filter(lambda x: x["gsheet_name"] in batch_nos,
                   self.gsheet_url_info))
        self.tool_thread = ToolThread(source_gsheet_url_info,
                                      clear_cache=False)
        self.tool_thread.finished.connect(self.on_tool_finished)
        self.tool_thread.start()

    def stop_tool(self):
        if hasattr(self, "tool_thread") and self.tool_thread.isRunning():
            logging.info("正在中断操作...")
            self.tool_thread.stop()
            self.stop_button.setEnabled(False)

    def on_tool_finished(self, success, error_msg):
        self.run_button.setEnabled(True)
        self.stop_button.setEnabled(False)

        if success:
            logging.info("操作已完成")
            QMessageBox.information(self, "成功", "操作已完成")
        else:
            error_msg = f"运行时出错: {error_msg}"
            logging.error(error_msg)
            QMessageBox.critical(self, "错误", error_msg)

    def check_for_updates(self):
        self.update_button.setEnabled(False)
        self.update_button.setText("正在检查更新...")

        self.update_thread = UpdateCheckThread(VERSION_CHECK_URL,
                                               CURRENT_VERSION)
        self.update_thread.check_finished.connect(
            self.on_update_check_finished)
        self.update_thread.start()

    def on_update_check_finished(self, has_update: bool, latest_version: str,
                                 download_url: str, release_notes: str):
        self.update_button.setEnabled(True)
        self.update_button.setText(f"检查更新 (当前版本: v{CURRENT_VERSION})")

        if has_update:
            message = f"""
            <h3>发现新版本！</h3>
            <p><b>当前版本:</b> v{CURRENT_VERSION}</p>
            <p><b>最新版本:</b> v{latest_version}</p>
            <p><b>更新说明:</b></p>
            <p>{release_notes}</p>
            <p>是否立即下载更新？</p>
            """

            reply = QMessageBox.question(self, "发现新版本", message,
                                         QMessageBox.Ok | QMessageBox.Cancel,
                                         QMessageBox.Ok)

            if reply == QMessageBox.Ok and download_url:
                self.start_update_download(download_url, latest_version)
        else:
            if "检查失败" in release_notes:
                QMessageBox.warning(self, "检查更新", f"检查更新失败: {release_notes}")
            else:
                QMessageBox.information(self, "检查更新",
                                        f"当前已是最新版本 (v{CURRENT_VERSION})")

    def start_update_download(self, download_url: str, latest_version: str):
        self.update_button.setEnabled(False)
        self.update_button.setText("正在下载更新...")

        self.update_progress = QProgressDialog("正在下载更新...", "取消", 0, 100, self)
        self.update_progress.setWindowTitle("更新下载")
        self.update_progress.setWindowModality(Qt.WindowModal)
        self.update_progress.setValue(0)
        self.update_progress.show()

        self.update_download_thread = UpdateDownloadThread(
            download_url, latest_version)
        self.update_download_thread.progress.connect(self.on_update_progress)
        self.update_download_thread.finished.connect(
            self.on_update_download_finished)
        self.update_progress.canceled.connect(
            self.update_download_thread.cancel)
        self.update_download_thread.start()

    def on_update_progress(self, percent: int, downloaded: int, total: int):
        self.update_progress.setValue(percent)
        if total > 0:
            self.update_progress.setLabelText(
                f"正在下载更新... {percent}% ({downloaded // 1024} KB / {total // 1024} KB)"
            )

    def on_update_download_finished(self, success: bool, message: str,
                                    file_path: str):
        self.update_button.setEnabled(True)
        self.update_button.setText(f"检查更新 (当前版本: v{CURRENT_VERSION})")
        self.update_progress.close()

        if not success:
            QMessageBox.warning(self, "更新失败", message)
            return

        self.apply_update_and_restart(file_path)

    def apply_update_and_restart(self, new_file_path: str):
        exe_path = os.path.abspath(sys.argv[0])  # 获取exe文件的绝对路径
        exe_dir = os.path.dirname(exe_path)  # 获取exe文件的目录
        current_pid = os.getpid()  # 获取当前进程的pid

        if sys.platform.startswith("win"):
            script_path = os.path.join(tempfile.gettempdir(),
                                       "update_quick_matching.ps1")
            with open(script_path, "w", encoding="utf-8") as f:
                f.write(f"""
                Start-Sleep -Seconds 3
                Stop-Process -Id {current_pid} -Force -ErrorAction SilentlyContinue
                Remove-Item -Path "{exe_dir}" -Recurse -Force -ErrorAction SilentlyContinue
                Expand-Archive -Path "{new_file_path}" -DestinationPath "{os.path.dirname(exe_dir)}" -Force
                Start-Process -FilePath "{exe_path}"
                """)
            os.system(
                f'powershell -ExecutionPolicy Bypass -File "{script_path}"')
            sys.exit(0)
        else:
            QMessageBox.information(self, "更新完成", f"更新已下载到: {new_file_path}")

    def sync_remote_code(self):
        self.sync_code_button.setEnabled(False)
        self.sync_code_button.setText("同步中...")

        self.sync_thread = CodeSyncThread()
        self.sync_thread.sync_finished.connect(self.on_sync_code_finished)
        self.sync_thread.start()

    def on_sync_code_finished(self, success: bool, message: str):
        self.sync_code_button.setEnabled(True)
        self.sync_code_button.setText("同步代码")

        if success:
            QMessageBox.information(self, "同步完成", message)
        else:
            QMessageBox.warning(self, "同步失败", message)


def main() -> None:
    app = QApplication(sys.argv)
    window = QuickMatchingApp()
    window.show()
    sys.exit(app.exec_())
