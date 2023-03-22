from PyQt5.QtWidgets import QApplication, QWidget, QLabel, QLineEdit, QPushButton, QCalendarWidget, QTextEdit, QMessageBox
from PyQt5.QtGui import QColor, QPalette
from PyQt5.QtCore import Qt, QDate, QProcess
import sys

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        # Set palette with purple colors
        palette = QPalette()
        palette.setColor(QPalette.Window, QColor("#5a275f")) # deep purple
        palette.setColor(QPalette.WindowText, QColor("#ffffff")) # white
        palette.setColor(QPalette.Button, QColor("#8d5da1")) # light purple
        palette.setColor(QPalette.ButtonText, QColor("#ffffff")) # white
        palette.setColor(QPalette.Base, QColor("#ffffff")) # white
        palette.setColor(QPalette.Text, QColor("#000000")) # black
        self.setPalette(palette)

        # Create the widgets
        self.start_date_label = QLabel("Start Date:", self)
        self.start_date_label.move(20, 20)
        self.start_date_edit = QLineEdit(self)
        self.start_date_edit.move(160, 20)
        self.start_date_edit.setReadOnly(True)
        self.start_date_edit.setFocusPolicy(Qt.NoFocus)

        self.start_date_button = QPushButton("Select Date", self)
        self.start_date_button.move(20, 60)
        self.start_date_button.clicked.connect(self.show_calendar)

        self.duration_label = QLabel("Duration (days):", self)
        self.duration_label.move(20, 100)
        self.duration_edit = QLineEdit(self)
        self.duration_edit.move(160, 100)

        self.submit_button = QPushButton("Submit", self)
        self.submit_button.move(20, 140)
        self.submit_button.clicked.connect(self.submit)

        self.log_output_label = QLabel("Log Output:", self)
        self.log_output_label.move(20, 180)
        self.log_output_edit = QTextEdit(self)
        self.log_output_edit.setReadOnly(True)
        self.log_output_edit.setGeometry(20, 200, 360, 240)

        # Set window properties
        self.setGeometry(100, 100, 400, 500)
        self.setWindowTitle("Date Calculator")

    def show_calendar(self):
        self.calendar = QCalendarWidget(self)
        self.calendar.setWindowModality(Qt.ApplicationModal)
        self.calendar.clicked.connect(self.select_date)
        self.calendar.setGridVisible(True)
        self.calendar.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
        self.calendar.setGeometry(50, 50, 250, 200)
        self.calendar.show()

    def select_date(self, date):
        self.start_date_edit.setText(date.toString(Qt.ISODate))
        self.calendar.close()

    def submit(self):
        start_date_str = self.start_date_edit.text()
        duration_str = self.duration_edit.text()

        if start_date_str == "" or duration_str == "":
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText("Please fill out both fields")
            msg.setStandardButtons(QMessageBox.Ok)
            msg.exec_()
            return

        start_date = QDate.fromString(start_date_str, Qt.ISODate).toString(Qt.ISODate)
        duration_days = int(duration_str)

        cmd = f"python3 -u ../ConcertScraper.py --duration-days {duration_days} --filter-start-date \"{start_date}\""
        process = QProcess(self)
        process.readyReadStandardOutput.connect(self.update_log_output)
        process.start(cmd)

    def update_log_output(self):
        process = self.sender()
        output = process.readAllStandardOutput().data().decode()
        self.log_output_edit.insertPlainText(output)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
