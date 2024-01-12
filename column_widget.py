from PyQt5.QtWidgets import QTabWidget, QWidget, QVBoxLayout, QPushButton, QMessageBox
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QLabel, QCheckBox
import pandas as pd
import os
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtGui import QImage, QPixmap
from io import BytesIO

from neo4j_connector import Neo4jConnector


class ColumnWidget(QWidget):
    def __init__(self, column_name, parent):
        super().__init__()

        self.setStyleSheet("""
            ColumnWidget {
                background-color: #F1F1F1; /* Couleur de fond */
                color: #333; /* Couleur du texte */
            }

            QCheckBox {
                color: #555; /* Couleur du texte de la case à cocher */
            }

            /* Ajoutez d'autres sélecteurs pour personnaliser d'autres éléments */
            """)

        # Stockez le nom de la colonne comme un attribut de l'instance
        self.column_name = column_name
        self.parent_tab = parent
        self.histogramme_checkbox = None

        self.layout = QVBoxLayout(self)

        # Bouton pour le nom de la colonne
        column_label = QPushButton(column_name)
        self.layout.addWidget(column_label)

        # Widget pour les sous-onglets
        subtab_widget = QTabWidget(self)
        self.layout.addWidget(subtab_widget)

        # Créez un onglet pour les opérations
        operations_tab = QWidget()
        operations_layout = QVBoxLayout(operations_tab)
        if (self.parent_tab.current_dataframe[self.column_name].isna().all()):
            operations_layout.addWidget(QPushButton("Aucune operation disponible, colonne nulle."))
        else:
            # Options d'opérations
            # "Statistiques", "Valeur la plus fréquente", "Valeurs distinctes", "Valeurs manquantes", "Histogrammes", "box plot"

            self.valeur_distinctes_checkbox = QCheckBox("Valeur distinnctes")
            self.frequent_value_checkbox = QCheckBox("Valeur la plus fréquente")
            self.missing_values_checkbox = QCheckBox("Nombre de valeurs manquantes")

            # Ajoutez des signaux/emplacements pour traiter les options sélectionnées
            self.valeur_distinctes_checkbox.stateChanged.connect(self.handle_operation_checkbox)
            self.frequent_value_checkbox.stateChanged.connect(self.handle_operation_checkbox)
            self.missing_values_checkbox.stateChanged.connect(self.handle_operation_checkbox)

            operations_layout.addWidget(self.valeur_distinctes_checkbox)
            
            operations_layout.addWidget(self.frequent_value_checkbox)
            operations_layout.addWidget(self.missing_values_checkbox)

            if self.is_histogram_relevant():
                self.histogramme_checkbox = QCheckBox("Histogramme")
                self.histogramme_checkbox.stateChanged.connect(self.handle_operation_checkbox)
                operations_layout.addWidget(self.histogramme_checkbox)

            # self.parent_tab.current_dataframe[self.column_name]
            
            if pd.api.types.is_numeric_dtype(self.parent_tab.current_dataframe[self.column_name].dtype) and np.issubdtype(self.parent_tab.current_dataframe[self.column_name].dtype, np.number):
                self.box_plot_checkbox = QCheckBox("Box plot")
                self.statistiques_checkbox = QCheckBox("Statistiques")

                self.statistiques_checkbox.stateChanged.connect(self.handle_operation_checkbox)
                self.box_plot_checkbox.stateChanged.connect(self.handle_operation_checkbox)

                operations_layout.addWidget(self.box_plot_checkbox)
                operations_layout.addWidget(self.statistiques_checkbox)


            # Ajoutez l'onglet des opérations aux sous-onglets
            
            subtab_widget.addTab(operations_tab, "Opérations")

            # Bouton "Calculer"
            calculate_button = QPushButton("Calculer")
            calculate_button.clicked.connect(self.calculate_operations)
            self.layout.addWidget(calculate_button)

            # Bouton "Enregistrer"
            save_button = QPushButton("Enregistrer")
            save_button.clicked.connect(self.save_results)
            self.layout.addWidget(save_button)

            # Résultats des opérations
            self.result_label = QLabel()
            self.layout.addWidget(self.result_label)

            # Widget pour les images
            self.histogram_chart = QLabel()
            self.layout.addWidget(self.histogram_chart)
            self.box_plot_chart = QLabel()
            self.layout.addWidget(self.box_plot_chart)
            
            self.result_tab = []


    def is_histogram_relevant(self):
        data = self.parent_tab.current_dataframe[self.column_name]
        unique_values_count = len(data.unique())
        
        if isinstance(self.parent_tab.current_dataframe[self.column_name][0], np.floating) and unique_values_count > 100:
            return False
        # Vérifier si la colonne a suffisamment de valeurs uniques pour être significative
        
        if (unique_values_count < 5) or (unique_values_count > 100):
            return False

        return True

    def calculate_operations(self):
        if self.parent_tab.current_dataframe is not None and not (self.parent_tab.current_dataframe[self.column_name].isna().all()):
            self.histogram_chart.clear()
            self.box_plot_chart.clear()
            
            result_texts = []

            # Calcul de la valeur la plus fréquente
            if pd.api.types.is_numeric_dtype(self.parent_tab.current_dataframe[self.column_name].dtype) and np.issubdtype(self.parent_tab.current_dataframe[self.column_name].dtype, np.number):
                if self.statistiques_checkbox.isChecked():
                    result_texts.append(("min", self.parent_tab.current_dataframe[self.column_name].min()))
                    result_texts.append(("mean", self.parent_tab.current_dataframe[self.column_name].mean()))
                    result_texts.append(("max", self.parent_tab.current_dataframe[self.column_name].max()))
                    result_texts.append(("std", self.parent_tab.current_dataframe[self.column_name].std()))
                
                # Calcul du nombre de valeurs manquantes
                if self.box_plot_checkbox.isChecked():
                    chart_path = self.create_chart(chart="box plot")
                    result_texts.append(("box plot", chart_path))

            # Calcul du nombre de valeurs manquantes
            if self.valeur_distinctes_checkbox.isChecked():
                result_texts.append(("Valeurs distinctes", f"{', '.join(map(str, self.parent_tab.current_dataframe[self.column_name].unique()))}"))

            # Calcul de la valeur la plus fréquente
            if self.histogramme_checkbox is not None and self.histogramme_checkbox.isChecked():
                chart_path = self.create_chart(chart="histogram")
                result_texts.append(("histogramme", chart_path))

            # Calcul de la valeur la plus fréquente
            if self.frequent_value_checkbox.isChecked():
                frequent_value = self.parent_tab.current_dataframe[self.column_name].mode().iloc[0]
                result_texts.append(("Valeur la plus fréquente", frequent_value))

            # Calcul du nombre de valeurs manquantes
            if self.missing_values_checkbox.isChecked():
                missing_values_count = self.parent_tab.current_dataframe[self.column_name].isnull().sum()
                result_texts.append(("Nombre de valeurs manquantes", missing_values_count))


            self.result_tab.append([self.column_name, result_texts])

            # Mettre à jour le DataFrame des résultats
            self.result_tab = pd.DataFrame(self.result_tab, columns=['Colonne', 'Resultats'])

            # Affichage des résultats dans le label dédié
            self.result_label.setText("\n".join([f"{label}: {value}" for label, value in result_texts]))

            print(f"Calcul pour {self.column_name} : Done")
        else:
            print("dataframe non défini ou nul ou colonne nulle")


    def create_chart(self, chart, save_path="images"):
        data = self.parent_tab.current_dataframe[self.column_name]
        data_str = data.astype(str)
        figure, ax = plt.subplots(figsize=(4, 3))
        if chart == "histogram":
            ax.hist(data_str, bins='auto')
            ax.set_title(f'Histogramme de {self.column_name}')
        elif chart == "box plot":
            ax.boxplot(data)
            ax.set_title(f'Box plot de {self.column_name}')

        # Ajuster la taille du graphique pour qu'il s'adapte à l'espace disponible
        figure.tight_layout()

        canvas = FigureCanvas(figure)
        buffer = BytesIO()
        canvas.print_png(buffer)
        buffer.seek(0)

        image = QImage.fromData(buffer.read())
        pixmap = QPixmap.fromImage(image)


        if not os.path.exists(save_path):
            os.makedirs(save_path)
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Define the file name based on column, chart type, and date/time
        file_name = f"{self.column_name}_{chart.replace(' ', '_')}_{current_datetime}.png"

        image_path = os.path.join(save_path, file_name)

        try:
            if chart == "histogram":
                self.histogram_chart.setPixmap(pixmap)
                self.histogram_path = image_path
            if chart == "box plot":
                self.box_plot_chart.setPixmap(pixmap)
                self.box_plot_path = image_path

            plt.savefig(image_path)
            plt.close(figure)
            return image_path
        except Exception as e:
            print("type de données non adapté pour ", chart)

        # Save the figure
        


    def save_results(self):
        file_name = self.parent_tab.file_name
        file_path = self.parent_tab.file_path
        sheet_name = self.parent_tab.sheet_name
        if file_name != None and file_path!= None and  len(self.result_tab)>0:
            try:
                uri = "bolt://localhost:7687/test"
                user = "neo4j"
                password = "LogisticData"

                connector = Neo4jConnector()
                connector.connect(uri, user, password)
                print("*********", sheet_name)
                connector.store_in_db(file_name=file_name, file_path=file_path, results_df=self.result_tab, sheet_name=sheet_name)
                connector.close()

                # QMessageBox.information(None, "Success", "Data saved in Neo4j")

                print("saved for : ", self.column_name)
            except Exception as e:
                # Gérer les erreurs lors du chargement du fichier
                print(str(e))
                #QMessageBox.critical(self, "Données absentes", f"Erreur de fichier ou resultats vides : {str(e)}")

    def handle_operation_checkbox(self, state):
        sender = self.sender()
        if state == Qt.Checked:
            print(f"{sender.text()} sélectionné pour la colonne {self.column_name}")
        else:
            print(f"{sender.text()} désélectionné pour la colonne {self.column_name}")

    def get_valeur_distinctes_checkbox(self):
        return self.valeur_distinctes_checkbox

    def get_histogramme_checkbox(self):
        if self.histogramme_checkbox:
            return self.histogramme_checkbox
        return None

    def get_frequent_value_checkbox(self):
        return self.frequent_value_checkbox

    def get_missing_values_checkbox(self):
        return self.missing_values_checkbox
    
    def get_box_plot_checkbox(self):
        if pd.api.types.is_numeric_dtype(self.parent_tab.current_dataframe[self.column_name].dtype) and np.issubdtype(self.parent_tab.current_dataframe[self.column_name].dtype, np.number):
            return self.box_plot_checkbox
        
    def get_statistiques_checkbox(self):
        if pd.api.types.is_numeric_dtype(self.parent_tab.current_dataframe[self.column_name].dtype) and np.issubdtype(self.parent_tab.current_dataframe[self.column_name].dtype, np.number):
            return self.statistiques_checkbox
