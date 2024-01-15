

from neo4j import GraphDatabase
from datetime import datetime
import pandas as pd


class Neo4jConnector:

    def __init__(self):
        self._driver = None

        self.sim_column_query = ("""
                        MATCH (s:Source)-[:POSSEDE]->(t:Table)-[:CONTIENT]->(c:Colonne)
                        WITH s, t, COLLECT(DISTINCT toLower(c.nom)) AS nomsColonnes

                        WITH COLLECT({source: s, table: t, colonnes: nomsColonnes}) AS allSourcesNoms
                        UNWIND allSourcesNoms AS sourceNoms1
                        UNWIND allSourcesNoms AS sourceNoms2
                        WITH
                        sourceNoms1, sourceNoms2
                        WHERE sourceNoms1.source < sourceNoms2.source AND 
                            size([nom IN sourceNoms1.colonnes WHERE nom IN sourceNoms2.colonnes]) > 0
                        WITH
                        sourceNoms1.source AS source1,
                        sourceNoms1.table AS table1,
                        REDUCE(s = [], nom IN sourceNoms1.colonnes | 
                                CASE WHEN nom IN sourceNoms2.colonnes THEN s + nom ELSE s END) AS ColonnesCommunes,
                        sourceNoms2.source AS source2,
                        sourceNoms2.table AS table2

                        MATCH (source1)-[:POSSEDE]->(table1)-[:CONTIENT]->(c1:Colonne)-[:EFFECTUE]->(a1)
                        MATCH (source2)-[:POSSEDE]->(table2)-[:CONTIENT]->(c2:Colonne)-[:EFFECTUE]->(a2)
                        WHERE toLower(c1.nom) = toLower(c2.nom) AND 
                            toLower(c1.nom) IN ColonnesCommunes AND 
                            source1 < source2  

                        WITH
                        source1, table1, c1, a1,
                        source2, table2, c2, a2,
                        REDUCE(s = [], nom IN ColonnesCommunes | 
                                CASE WHEN toLower(c1.nom) = toLower(nom) THEN s + nom ELSE s END) AS UniqueNomColonne

                        WITH
                        source1, table1, a1,
                        source2, table2, c2, a2,
                        COLLECT(DISTINCT c1)[0] AS uniqueColonne

                        DETACH DELETE c2
                        MERGE (table2)-[:CONTIENT]->(uniqueColonne)-[:EFFECTUE]->(a2)

                        RETURN source1, table1, uniqueColonne, a1, source2, table2, a2
                    """
                    )
        self.integrate = """
            WITH s1, t1, c1, s2, t2, c2, type_integration
            

        """

    def close(self):
         if self._driver is not None:
            self._driver.close()

    def connect(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def integrate_by_name(self):
        typeIntegration = "Analyse par nom des colonnes"
        with self._driver.session() as session:
                # Begin a transaction
                with session.begin_transaction() as tx:
                    sim_col_names = tx.run(self.sim_column_query)
                    for record in sim_col_names:
                         print(record)
                    return sim_col_names, typeIntegration
    
    
    def integrate_by_correspondance(self):
        # Chemin vers votre fichier CSV
        csv_file_path = "C:\\Users\\alhaj\\Downloads\\Analyse_Donnees_Logistiques-main\\Analyse_Donnees_Logistiques-main\\correspondace.csv"
        # Lecture du fichier CSV avec pandas
        df_correspond = pd.read_csv(csv_file_path)

        df_correspond= pd.DataFrame(df_correspond, columns=['source1', 'column1', 'source2', 'column2'])

        for index, row in df_correspond.iterrows():
            source1 = row['source1']
            column1 = row['column1']
            source2 = row['source2']
            column2 = row['column2']

        typeIntegration="Analyse par table de correspondance"

        s1=[]
        s2=[]
        c1=[]
        c2=[]
        t1=[]
        t2=[]

            # Créer un lien entre la source et la colonne dans Neo4j
        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                query = (
                    """
                    MATCH (s1:Source {nom: $source1})-[:POSSEDE]->(t1:Table)-[:CONTIENT]->(c1:Colonne {nom: $column1})
                    MATCH (s2:Source {nom: $source2})-[:POSSEDE]->(t2:Table)-[:CONTIENT]->(c2:Colonne {nom: $column2})
                    RETURN s1, t1, c1, s2, t2, c2
                    """
                )
                result = tx.run(query, source1=source1, column1=column1, source2=source2, column2=column2)

                for res in result:
                    s1.append(str(res['s1']))
                    t1.append(str(res['t1']))
                    c1.append(str(res['c1']))
                    s2.append(str(res['s2']))
                    t2.append(str(res['t2']))
                    c2.append(str(res['c2']))
                    
        print("Analyse par correspondance finie")
        return s1, t1, c1, s2, t2, c2, typeIntegration
    
    
    def integrate_by_analysis_profiling(self):
        typeIntegration = "Analyse des profils"
        with self._driver.session() as session:
                with session.begin_transaction() as tx:
                    
                    #Récupération des noeuds de type Analyse dans le graphe Neo4J
                    analyse_query = (
                        "MATCH (a:Analyse) "
                        "RETURN a"
                    )
                    analyses = list(tx.run(analyse_query))
                    print("*** " + str(len(analyses)) + " analyses trouvées ***")

                    sum = 0
                    idNoeud1 = []
                    idNoeud2 = []
                    analyse1 = []
                    analyse2 = []
                    source1 = []
                    source2 = []
                    table1 = []
                    table2 = []
                    colonne1 = []
                    colonne2 = []
                    
                    #Comparaison des propriétés des noeuds Analyse 2 à 2
                    for i in range(len(analyses)):
                        for j in range(i+1, len(analyses)):
                            noeud1 = analyses[i]['a']
                            noeud2 = analyses[j]['a']
                            
                            #Propriete : Valeurs distinctes
                            distinct1 = set(noeud1['Valeurs distinctes'].split(', '))
                            distinct2 = set(noeud2['Valeurs distinctes'].split(', '))
                            valCommunes = distinct1.intersection(distinct2)
                        
                            #Propriete : Valeur la plus fréquente
                            frequente1 = noeud1['Valeur la plus fréquente']
                            frequente2 = noeud2['Valeur la plus fréquente']

                            # Valeur la plus fréquente ET Valeurs distinctes identiques
                            if valCommunes and (frequente1==frequente2):
                                sum=sum+1
                                idNoeud1.append(noeud1.id)
                                idNoeud2.append(noeud2.id)
                    print("*** "+str(sum)+" liens trouvés ***")
                    print("*** "+str(len(idNoeud1))+" noeuds 1 trouvés ***")
                    print("*** "+str(len(idNoeud2))+" noeuds 2 trouvés ***")
                    
                    it = 0
                    for i in range(len(idNoeud1)):
                        profil_query1 = (
                            "MATCH (s:Source)-[:POSSEDE]->(t:Table)-[:CONTIENT]->(c:Colonne)-[:EFFECTUE]->(a:Analyse) "
                            "WHERE id(a) = $idAnalyse "
                            "RETURN a, c, t, s"
                        )
                        
                        r1 = tx.run(profil_query1, idAnalyse=idNoeud1[i])
                        #resultat1.extend(r1)
                        
                        for res1 in r1:
                            analyse1.append(str(res1['a']))
                            source1.append(str(res1['s']))
                            table1.append(str(res1['t']))
                            colonne1.append(str(res1['c']))
                            it=it+1
                        
                    
                    for j in range(len(idNoeud2)):
                        profil_query2 = (
                            "MATCH (s:Source)-[:POSSEDE]->(t:Table)-[:CONTIENT]->(c:Colonne)-[:EFFECTUE]->(a:Analyse) "
                            "WHERE id(a) = $idAnalyse "
                            "RETURN a, c, t, s"
                        )
                        r2 = tx.run(profil_query2, idAnalyse=idNoeud2[j])
                        #resultat2.extend(r2)
                        
                        for res2 in r2:
                            analyse2.append(str(res2['a']))
                            source2.append(str(res2['s']))
                            table2.append(str(res2['t']))
                            colonne2.append(str(res2['c']))
                            it=it+1
                    
                    
                    
                    #Affichage des résultats
                    print("\nSIMILITUDE : "+str(int(it/2))+" cas")

        print("Analyse profil terminée")
                   
        return source1, table1, colonne1, source2, table2, colonne2, typeIntegration
               
                    
    def store_in_db(self, file_name, file_path, results_df, sheet_name=None):
        if not (results_df.empty and file_name.empty and results_df.empty):
            with self._driver.session() as session:
                # Begin a transaction
                with session.begin_transaction() as tx:
                    # Créer le nœud Source
                    source = file_name
                    if sheet_name != None:
                        table = sheet_name
                    else:
                        table = file_name

                    source_query = (
                        "MERGE (s:Source {nom: $source_name, chemin: $file_path})"
                        )
                    tx.run(source_query, source_name=source, file_path=file_path)

                    # Créer la relation POSSEDE s'il n'existe pas
                    posede_query = (
                    "MATCH (s:Source {nom: $source_name, chemin: $file_path}) "
                    "MERGE (s)-[:POSSEDE]->(t:Table {nom: $table_name})"
                    )

                    tx.run(posede_query, source_name=source, file_path=file_path, table_name=table)


                    # Parcourir les résultats et les stocker dans Neo4j
                    #for _, row in results_df.iterrows():
                    column = results_df['Colonne'][0]
                    print(f"SAVE -- nom : {column} - type : {type(column)}")
                    all_resultats = results_df['Resultats']

                    # Créer la relation CONTIENT
                    contient_query = (
                        "MATCH (s:Source{nom:$source_name})-[:POSSEDE]->(t:Table {nom: $table_name})"
                        "MERGE (t)-[:CONTIENT]->(c:Colonne {nom: $column_name})"
                    )
                    tx.run(contient_query, source_name=source, table_name=table, column_name=column)

                   # Create the relationship EFFECTUE query
                    effectue_query = (
                        " MATCH (s:Source {nom: $source_name})-[:POSSEDE]->(t:Table {nom: $table_name})-[:CONTIENT]->(c:Colonne {nom: $column_name}) "
                        " MERGE (c)-[:EFFECTUE]->(a:Analyse) "
                        " SET "
                    )

                    # Add dynamic SET clause based on resultats DataFrame
                    for resultats in all_resultats:
                        for result in resultats:
                            # Enclose both parameter name and value in backticks
                            parameter_name = f"`{result[0]}`"
                            parameter_value = result[1]
                            effectue_query += f"a.{parameter_name} = '{parameter_value}', "

                        # Remove the trailing comma
                        effectue_query = effectue_query.rstrip(', ')

                    # Run the Cypher query
                    tx.run(effectue_query, source_name=source, table_name=table, column_name=column)
                    
                    # Vérifier si il y a un profilage ou non
                    profilage_query = (
                        "MATCH (s:Source {nom: $source_name, chemin: $file_path})-[:ACCEDE]->(p:Profilage {source: $source_profilage}) "
                        f"RETURN COUNT(*) AS count"
                    )
                    verification = tx.run(profilage_query, source_name=source, file_path=file_path, source_profilage=source)
                    
                    if(verification.single()["count"] == 0):
                        # Créer le nœud Profilage (avec dateCreation) et la relation ACCEDE
                        accede_query = (
                            "MATCH (s:Source {nom: $source_name, chemin: $file_path})-[:POSSEDE]->(t:Table {nom: $table_name}) "
                            f"MATCH (t)-[:CONTIENT]->(c:Colonne) "
                            f"MATCH (c)-[:EFFECTUE]->(a:Analyse) "
                            "MERGE (p:Profilage {source: $source_profilage, date_creation: $date_creation}) "
                            f"MERGE (s)-[:ACCEDE]->(p)"
                        )   
                        tx.run(accede_query, source_name=source, file_path=file_path, table_name=table, source_profilage=source, date_creation=datetime.now().strftime("%d/%m/%Y_%H:%M:%S"))
    
                    # Créer la relation STOCKE + modifier la date de MAJ dans Profilage
                    stocke_query = (
                        "MATCH (s:Source {nom: $source_name, chemin: $file_path})-[:POSSEDE]->(t:Table {nom: $table_name}) "
                        f"MATCH (t)-[:CONTIENT]->(c:Colonne) "
                        f"MATCH (c)-[:EFFECTUE]->(a:Analyse) "
                        "MATCH (s)-[:ACCEDE]->(p:Profilage {source: $source_profilage})"
                        f"MERGE (a)-[:STOCKE]->(p)"
                        f"SET p.date_maj= $date_maj"
                    )
                    tx.run(stocke_query, source_name=source, file_path=file_path, table_name=table, source_profilage=source, date_maj=datetime.now().strftime("%d/%m/%Y_%H:%M:%S"))