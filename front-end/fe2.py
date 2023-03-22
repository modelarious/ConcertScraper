from PyQt5.QtCore import QObject, QThread, pyqtSignal
from PyQt5.QtGui import QTextCursor
from PyQt5.QtWidgets import (
    QApplication,
    QMainWindow,
    QPushButton,
    QVBoxLayout,
    QWidget,
    QTextEdit,
)
import sys
import subprocess


class StreamThread(QThread):
    update = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.process = None

    def run(self):
        self.process = subprocess.Popen(
            ["python3", "-u", "hello.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        while True:
            output = self.process.stdout.readline().decode().strip()
            if output:
                self.update.emit(output)
            else:
                break

    def stop(self):
        if self.process:
            self.process.kill()


class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.text_edit = QTextEdit()
        self.submit_button = QPushButton("Submit")
        self.submit_button.clicked.connect(self.submit)

        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.addWidget(self.text_edit)
        layout.addWidget(self.submit_button)

        self.setCentralWidget(widget)

        self.thread = StreamThread()
        self.thread.update.connect(self.update_text_edit)

    def submit(self):
        self.thread.start()

    def update_text_edit(self, text):
        self.text_edit.moveCursor(QTextCursor.End)
        self.text_edit.insertPlainText(text + "\n")

    def closeEvent(self, event):
        self.thread.stop()
        self.thread.wait()
        event.accept()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
