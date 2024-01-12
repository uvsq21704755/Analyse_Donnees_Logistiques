

from neo4j import GraphDatabase


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

    def close(self):
         if self._driver is not None:
            self._driver.close()

    def connect(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def integrate_by_name(self):
        with self._driver.session() as session:
                # Begin a transaction
                with session.begin_transaction() as tx:
                    sim_col_names = tx.run(self.sim_column_query)
                    for record in sim_col_names:
                         print(record)
                    return sim_col_names
                    
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
