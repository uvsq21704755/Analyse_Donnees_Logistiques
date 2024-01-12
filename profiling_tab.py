
from PyQt5.QtWidgets import QTabWidget, QWidget, QVBoxLayout, QPushButton, QFileDialog, QScrollArea, QInputDialog, QLineEdit
import pandas as pd
import os
import numpy as np

from column_widget import ColumnWidget

class ProfilingTab(QWidget):
    def __init__(self):
        super().__init__()
        self.current_dataframe = None

        layout = QVBoxLayout(self)

        self.load_button = QPushButton("Charger un fichier CSV ou Excel")
        self.load_button.clicked.connect(self.load_file)
        layout.addWidget(self.load_button)

        # Créez un QScrollArea et configurez-le pour être redimensionnable
        scroll_area = QScrollArea(self)
        scroll_area.setWidgetResizable(True)

        # Créez le widget QTabWidget et configurez-le
        self.column_tabs = QTabWidget(scroll_area)

        # Ajoutez vos onglets au QTabWidget ici

        # Définissez le widget à faire défiler
        scroll_area.setWidget(self.column_tabs)

        # Ajoutez le QScrollArea au layout
        layout.addWidget(scroll_area)

        self.select_all_button = QPushButton("Tout sélectionner")
        self.select_all_button.clicked.connect(self.select_all_columns)
        layout.addWidget(self.select_all_button)

        self.calculate_all_button = QPushButton("Tout calculer")
        self.calculate_all_button.clicked.connect(self.calculate_all_columns)
        layout.addWidget(self.calculate_all_button)

        self.save_all_button = QPushButton("Tout enregistrer")
        self.save_all_button.clicked.connect(self.save_all_columns)
        layout.addWidget(self.save_all_button)

        self.sheet_name = None


    def load_file(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        file_path, _ = QFileDialog.getOpenFileName(self, "Choisir un fichier", "", "Fichiers CSV (*.csv);;Fichiers Excel (*.xlsx *.xls);;Tous les fichiers (*)", options=options)
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        if file_path:
            try:
                # Charger les données du fichier en utilisant pandas
                if file_path.lower().endswith('.csv'):
                    try:
                        self.current_dataframe = pd.read_csv(file_path, encoding='utf-8')
                    except UnicodeDecodeError:
                        self.current_dataframe = pd.read_csv(file_path, encoding='ISO-8859-1')
                elif file_path.lower().endswith(('.xls', '.xlsx')):
                    # Lire les noms des feuilles
                    sheet_names = pd.ExcelFile(file_path).sheet_names
                    selected_sheet, ok_pressed = QInputDialog.getItem(self, "Choisir une feuille", "Feuilles disponibles:", sheet_names, 0, False)
                    
                    if ok_pressed and selected_sheet:
                        # Charger la feuille sélectionnée
                        self.current_dataframe = pd.read_excel(file_path, sheet_name=selected_sheet)
                        self.sheet_name = selected_sheet

                        # Afficher les colonnes du DataFrame
                self.show_columns(self.current_dataframe)

            except Exception as e:
                # Gérer les erreurs lors du chargement du fichier
                print(str(e))

    def show_columns(self, dataframe):
        self.column_tabs.clear()
        self.column_widgets = []

        for column in dataframe.columns:
            
            tab = QWidget()
            tab_layout = QVBoxLayout(tab)

            # Utilisez un QHBoxLayout pour mettre le nom de la colonne horizontalement
            column_widget = ColumnWidget(column, self)
            tab_layout.addWidget(column_widget)
            self.column_widgets.append(column_widget)

            # Ajoutez vos fonctionnalités spécifiques pour chaque colonne ici

            self.column_tabs.addTab(tab, column)

            # Utilisez le style CSS pour orienter le texte horizontalement
            self.column_tabs.setStyleSheet("QTabBar::tab {writing-mode: horizontal-tb }")
            self.column_tabs.setTabPosition(QTabWidget.West)



    def select_all_columns(self):
        for column_widget in self.column_widgets:
            if not (self.current_dataframe[column_widget.column_name].isna().all()):
                column_widget.get_valeur_distinctes_checkbox().setChecked(True)
                
                column_widget.get_frequent_value_checkbox().setChecked(True)
                column_widget.get_missing_values_checkbox().setChecked(True)

                if column_widget.get_histogramme_checkbox() is not None:
                    column_widget.get_histogramme_checkbox().setChecked(True)
                if pd.api.types.is_numeric_dtype(self.current_dataframe[column_widget.column_name].dtype) and np.issubdtype(self.current_dataframe[column_widget.column_name].dtype, np.number):
                    column_widget.get_box_plot_checkbox().setChecked(True)
                    column_widget.get_statistiques_checkbox().setChecked(True)

    def calculate_all_columns(self):
        # Parcourez toutes les colonnes et appelez la méthode calculate_operations
        for column_widget in self.column_widgets:
            if not (self.current_dataframe[column_widget.column_name].isna().all()):
                column_widget.calculate_operations()

    def save_all_columns(self):
        # Parcourez toutes les colonnes et appelez la méthode save_results
        for column_widget in self.column_widgets:
            if not (self.current_dataframe[column_widget.column_name].isna().all()):
                column_widget.save_results()