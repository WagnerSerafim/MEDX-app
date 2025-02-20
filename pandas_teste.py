import sys
import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QLabel, QLineEdit,
    QPushButton, QFileDialog, QMessageBox, QProgressBar
)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime


class MigrationApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Migrador de Pacientes")
        self.setGeometry(100, 100, 400, 300)

        layout = QVBoxLayout()

        self.label_sid = QLabel("SoftwareID:")
        self.input_sid = QLineEdit()
        layout.addWidget(self.label_sid)
        layout.addWidget(self.input_sid)

        self.label_password = QLabel("Senha:")
        self.input_password = QLineEdit()
        self.input_password.setEchoMode(QLineEdit.EchoMode.Password)
        layout.addWidget(self.label_password)
        layout.addWidget(self.input_password)

        self.label_db = QLabel("Database:")
        self.input_db = QLineEdit()
        layout.addWidget(self.label_db)
        layout.addWidget(self.input_db)

        self.btn_file = QPushButton("Selecionar Arquivo Excel")
        self.btn_file.clicked.connect(self.select_file)
        layout.addWidget(self.btn_file)

        self.btn_start = QPushButton("Iniciar Migração")
        self.btn_start.clicked.connect(self.start_migration)
        layout.addWidget(self.btn_start)

        self.progress_bar = QProgressBar()
        layout.addWidget(self.progress_bar)

        self.setLayout(layout)

        self.file_path = ""

    def select_file(self):
        file_dialog = QFileDialog()
        file_name, _ = file_dialog.getOpenFileName(self, "Selecione o arquivo Excel", "", "Arquivos Excel (*.xlsx)")
        if file_name:
            self.file_path = file_name
            QMessageBox.information(self, "Arquivo Selecionado", f"Arquivo escolhido:\n{file_name}")

    def start_migration(self):
        if not self.file_path or not self.input_sid.text() or not self.input_password.text() or not self.input_db.text():
            QMessageBox.warning(self, "Erro", "Preencha todos os campos e selecione um arquivo!")
            return

        try:
            sid = self.input_sid.text()
            password = self.input_password.text()
            dbase = self.input_db.text()

            DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server"
            engine = create_engine(DATABASE_URL)

            Base = automap_base()
            Base.prepare(engine, reflect=True)

            SessionLocal = sessionmaker(bind=engine)
            session = SessionLocal()

            Contatos = Base.classes.Contatos

            df = pd.read_excel(self.file_path)

            total = len(df)
            self.progress_bar.setValue(0)

            for index, row in df.iterrows():
                if pd.isna(row["NASCIMENTO"]) or row["NASCIMENTO"] == "":
                    birthday = datetime.strptime("01/01/1900 00:00", "%d/%m/%Y %H:%M")
                else:
                    birthday = row["NASCIMENTO"]

                sex = "M" if pd.isna(row["SEXO"]) or row["SEXO"] == "" else row["SEXO"]

                new_contact = Contatos(
                    Nome=str(row["NOME"])[:50],
                    Nascimento=birthday,
                    Sexo=sex,
                    RG=str(row["RG"])[:25],
                    Email=str(row["EMAIL"])[:100]
                )
                setattr(new_contact, "Id do Cliente", row["ID_PACIENTE"])
                setattr(new_contact, "CPF/CGC", str(row["CPF"])[:25])

                session.add(new_contact)

                self.progress_bar.setValue(int((index + 1) / total * 100))

            session.commit()
            session.close()

            QMessageBox.information(self, "Sucesso", "Migração concluída com sucesso!")
            self.progress_bar.setValue(100)

        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro na migração: {str(e)}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MigrationApp()
    window.show()
    sys.exit(app.exec())
