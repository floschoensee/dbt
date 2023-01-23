#!/bin/python3.10
"""
@author Florian Schönsee, s940485@bht-berlin.de
How-To am Ende der Datei
"""
import csv
from operator import mod
import sys
import timeit
from neo4j import GraphDatabase

class App:
    """
    Initialize Database Connection. Username and Password are given in __main__
    """
    def __init__(self, uri, database_username, database_password):
        self.driver = GraphDatabase.driver(uri, auth=(database_username, database_password))
    """
    Close Database Connection
    """
    def close(self):
        self.driver.close()
    """
    create Database Scheme consisting of: 1 University Node, 1 Faculty Node, 1 course, 1 module, and three students. this method calls the cypher-method for each node to be created.
    """
    def create_database_model(self):
        with self.driver.session() as session:
           session.write_transaction(self.cypher_create_university,"001","VFH") 
           session.write_transaction(self.cypher_create_faculty,"001")
           session.write_transaction(self.cypher_create_course,"Medieninformatik Master","MMIO", "4")
           session.write_transaction(self.cypher_create_module,"DBT", "Datenbanktechnologien", "5")
           session.write_transaction(self.cypher_create_student, "999999", "Heike")
           session.write_transaction(self.cypher_create_student, "111111", "Inggita")
           session.write_transaction(self.cypher_create_student, "555555", "Florian")
           session.write_transaction(self.cypher_link_uni_to_faculty,"VFH","001")
           session.write_transaction(self.cypher_link_uni_to_course,"VFH","MMIO")
           session.write_transaction(self.cypher_link_faculty_to_course,"001","MMIO")
           session.write_transaction(self.cypher_link_course_to_module,"MMIO","DBT")
           session.write_transaction(self.cypher_link_student_to_course,"111111","MMIO")
           session.write_transaction(self.cypher_link_student_to_course,"999999","MMIO")
           session.write_transaction(self.cypher_link_student_to_course,"555555","MMIO")
           session.write_transaction(self.cypher_link_student_to_module,"111111","DBT")
           session.write_transaction(self.cypher_link_student_to_module,"999999","DBT")
           session.write_transaction(self.cypher_link_student_to_module,"555555","DBT")



    """
    Cypher Method for the university node. creates the university with the given Details in create_database_model
    """
    @staticmethod
    def cypher_create_university(tx, university_id, university_name):
        query = (
            "MERGE (u:university {university_id: $university_id, university_name: $university_name})"
        )
        tx.run(query, university_id=university_id, university_name=university_name)
    """
    Cypher Method for the faculty node
    """
    @staticmethod
    def cypher_create_faculty(tx, faculty_id):
        query = (
            "MERGE (u:faculty {faculty_id: $faculty_id})"
        )
        tx.run(query, faculty_id=faculty_id)
    """
    Cypher Method for the course (studiengang) node
    """
    @staticmethod
    def cypher_create_course(tx, course_name, course_short, number_of_regular_semesters):
        query = (
            "MERGE (u:course { course_name: $course_name, course_short: $course_short, number_of_regular_semesters: $number_of_regular_semesters})"
        )
        tx.run(query, course_name=course_name, course_short=course_short, number_of_regular_semesters=number_of_regular_semesters)
    """
    Cypher Method for the module node
    """
    @staticmethod
    def cypher_create_module(tx, module_short, module_name, credits):
        query = (
            "MERGE (u:module {module_short: $module_short, module_name: $module_name, credits: $credits})"
        )
        tx.run(query, module_short=module_short, module_name=module_name, credits=credits)

    """
    Cypher Method for the faculty node
    """
    @staticmethod
    def cypher_create_student(tx, student_id, student_name):
        query = (
            "MERGE (u:student {student_id: $student_id, student_name: $student_name})"
        )
        tx.run(query, student_id=student_id, student_name=student_name)

    @staticmethod
    def cypher_link_uni_to_faculty(tx, university_name, faculty_id):
        query = (
            "MATCH (uni:university{university_name: $university_name}), (fac:faculty{faculty_id: $faculty_id}) "
            "MERGE (uni)-[r:offers]-(fac)"
        )
        tx.run(query, university_name=university_name, faculty_id=faculty_id)
    
    @staticmethod
    def cypher_link_faculty_to_course(tx, faculty_id, course_short):
        query = (
            "MATCH (c:course{course_short: $course_short}), (fac:faculty{faculty_id: $faculty_id}) "
            "MERGE (c)-[r:belongs_to]-(fac)"
        )
        tx.run(query, course_short=course_short, faculty_id=faculty_id)

    @staticmethod
    def cypher_link_course_to_module(tx, course_short, module_short):
        query = (
            "MATCH (c:course{course_short: $course_short}), (mod:module{module_short: $module_short}) "
            "MERGE (mod)-[r:is_part_of]-(c)"
        )
        tx.run(query, course_short=course_short, module_short=module_short)

    @staticmethod
    def cypher_link_student_to_module(tx, student_id, module_short):
        query = (
            "MATCH (s:student{student_id: $student_id}), (mod:module{module_short: $module_short}) "
            "MERGE (s)-[r:visits]-(mod)"
        )
        tx.run(query, student_id=student_id, module_short=module_short)

    @staticmethod
    def cypher_link_student_to_course(tx, student_id, course_short):
        query = (
            "MATCH (s:student{student_id: $student_id}), (c:course{course_short: $course_short}) "
            "MERGE (s)-[r:is_student_of]-(c)"
        )
        tx.run(query, student_id=student_id, course_short=course_short)

    @staticmethod
    def cypher_link_uni_to_course(tx, university_name, course_short):
        query = (
            "MATCH (uni:university{university_name: $university_name}), (c:course{course_short: $course_short}) "
            "MERGE (uni)-[r:offers]-(c)"
        )
        tx.run(query, university_name=university_name, course_short=course_short)

    
    






if __name__ == "__main__":
    start_time = timeit.default_timer()
    bolt_url = "bolt://diskstation:7687"
    user = "neo4j"
    password = "3Kd0j3zT6tcehoHXRcH7PCQi"
    app = App(bolt_url, user, password)
    print(" [###] hello!\n [###] let's import some data!\n ")
    print(" [###] creating university!")
    app.create_database_model()
    app.close()
    stop_time = timeit.default_timer()
    print(" [###] time elapsed: ", stop_time - start_time, "seconds")



"""
HOW TO:
Dieses Programm wurde auf Basis des Neo4j Docker-Containers getestet. Um den Container zu starten ist das erzeugebn dieser Ordnerstruktur im homeverzeichnis des users erforderlich, außerdem muss natürlich Docker auf der betreffenden Workstation installiert sein.
    mkdir -p $HOME/neo4j/data $HOME/neo4j/logs $HOME/neo4j/import $HOME/neo4j/plugins

Der Container kann dann mit dem folgenden Befehl gestartet werden:
    docker run -p 7474:7474 -p 7687:7687 -d \
    -v $HOME/neo4j/data:/data -v $HOME/neo4j/logs:/logs \
    -v $HOME/neo4j/import:/var/lib/neo4j/import \
    -v $HOME/neo4j/plugins:/var/lib/neo4j/plugins \
    --env NEO4J_AUTH=neo4j/3Kd0j3zT6tcehoHXRcH7PCQi neo4j:latest

Dann ist obige das Programm lauffähig. Die Weboberfläche ist dann über HOSTNAME:7474 im Browser erreichbar. In diesem Fall ist der Host ein Synology-NAS. Der Hostname muss auch in der bolt_url eingetragen werden.
Im Webinterface kann mithilfe von 

MATCH (n) 
RETURN n 

der gesamte Graph angezeigt werden.

"""