#!/usr/bin/python3
# Metadata.py
# Ashish D'Souza (computer_geek64 or computer-geek64)
# June 10th, 2019

import os
import mysql.connector as sql
from getpass import getpass


class Metadata:
    def __init__(self, directory: str, tags: tuple, **kwargs):
        self.directory = directory
        self.tags = tuple(["[" + tag + "]" for tag in tags])
        if "user" not in kwargs.keys() and "host" not in kwargs.keys():
            self.user, self.host = input("user@host >> ").split("@")
        else:
            self.user = kwargs["user"] if "user" in kwargs.keys() else input("user >> ")
            self.host = kwargs["host"] if "host" in kwargs.keys() else input("host >> ")
        self.password = kwargs["password"] if "password" in kwargs.keys() else getpass(self.user + "@" + self.host + "'s password: ")
        self.database = kwargs["database"] if "database" in kwargs.keys() else input("database >> ")
        self.table = kwargs["table"] if "table" in kwargs.keys() else input("table >> ")
        self.sql_connection = sql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.sql_cursor = self.sql_connection.cursor()

    def create_all(self, overwrite=False, **kwargs) -> None:
        dirs = [dirs for root, dirs, files in os.walk(self.directory) if dirs][0]
        for dir in dirs:
            self.create(dir, overwrite, **kwargs)

    def create(self, dir: str, overwrite=False, **kwargs) -> None:
        if not os.path.exists(os.path.join(self.directory, dir, "metadata.txt")) or overwrite:
            # Create new metadata file with specified default values
            with open(os.path.join(self.directory, dir, "metadata.txt"), "w") as metadata_file:
                output = ""
                for tag in self.tags:
                    output += tag + "\n"
                    if tag[1:-1] in kwargs.keys():
                        for value in kwargs[tag[1:-1]]:
                            output += value + "\n"
                    output += "\n"
                metadata_file.write(output)
        else:
            # Update existing metadata file with specified default values
            with open(os.path.join(self.directory, dir, "metadata.txt"), "r") as metadata_file:
                metadata_lines = [line.lower().strip() for line in metadata_file.readlines()]
            for tag in self.tags:
                if tag not in metadata_lines:
                    with open(os.path.join(self.directory, dir, "metadata.txt"), "a") as metadata_file:
                        output = tag + "\n"
                        if tag[1:-1] in kwargs.keys():
                            for value in kwargs[tag[1:-1]]:
                                output += value + "\n"
                        output += "\n"
                        metadata_file.write(output)

    def remove_all(self):
        dirs = [dirs for root, dirs, files in os.walk(self.directory) if dirs][0]
        for dir in dirs:
            self.remove(dir)

    def remove(self, dir: str):
        if os.path.exists(os.path.join(self.directory, dir, "metadata.txt")):
            os.remove(os.path.join(self.directory, dir, "metadata.txt"))

    def get_unknown(self):
        unknown = []
        dirs = [dirs for root, dirs, files in os.walk(self.directory) if dirs][0]
        for dir in dirs:
            with open(os.path.join(self.directory, dir, "metadata.txt"), "r") as metadata_file:
                metadata_lines = metadata_file.readlines()
            unknown += [metadata_lines[i - 1].strip() + " " + dir for i in range(1, len(metadata_lines)) if metadata_lines[i] == "\n" and metadata_lines[i - 1].startswith("[")]
        return unknown

    def update_table(self):
        self.sql_cursor.execute("DROP TABLE " + self.table + ";")
        #self.sql_cursor.fetchall()
        self.sql_cursor.execute("CREATE TABLE " + self.table + " (" + ", ".join([tag[1:-1] + " text" for tag in self.tags]) + ");")
        #self.sql_cursor.fetchall()
        dirs = [dirs for root, dirs, files in os.walk(self.directory) if dirs][0]
        for dir in dirs:
            self.update_table_values(dir)
        # Add FULLTEXT INDEX
        self.sql_cursor.execute("CREATE FULLTEXT INDEX fulltextindex ON " + self.table + " (" + ", ".join(map(lambda x: x[1:-1], self.tags)) + ");")
        #self.sql_cursor.fetchall()

        """
        self.update_table_columns(dir, self.table)
        # Remove empty columns
        self.sql_cursor.execute("SELECT COUNT(*) FROM " self.table + ";")
        rows = self.sql_cursor.fetchall()[0][0]
        for column in self.get_table_columns():
            self.sql_cursor.execute("SELECT COUNT(*) FROM " + self.table + " WHERE " + column + " IS NULL OR " + column + "=\"\";")
            if self.sql_cursor.fetchall()[0][0] == rows:
                # Drop column
                self.sql_cursor.execute("ALTER TABLE " + self.table + " DROP " + column + ";")

    def update_table_columns(self, dir: str):
        if os.path.exists(os.path.join(self.directory, dir, "metadata.txt")):
            with open(os.path.join(self.directory, dir, "metadata.txt"), "r") as metadata_file:
                metadata_lines = metadata_file.readlines()
            tags = [line.strip()[1:-1] for line in metadata_lines if line.startswith("[")]
            columns = self.get_table_columns()
            for tag in tags:
                if tag not in columns:
                    self.sql_cursor.execute("ALTER TABLE " + self.table + " ADD " + tag + " text;")
                    self.sql_cursor.fetchall()

    def get_table_columns(self):
        self.sql_cursor.execute("DESC " + self.table + ";")
        return [x[0] for x in cursor.fetchall()]
    """

    def update_table_values(self, dir: str):
        if os.path.exists(os.path.join(self.directory, dir, "metadata.txt")):
            with open(os.path.join(self.directory, dir, "metadata.txt"), "r") as metadata_file:
                metadata_lines = metadata_file.readlines()
            values = {}
            for tag in self.tags:
                values[tag[1:-1]] = " ".join(map(lambda x: x.strip(), metadata_lines[metadata_lines.index(tag + "\n") + 1:metadata_lines[metadata_lines.index(tag + "\n"):].index("\n")]))
                print(values[tag[1:-1]])
                values[tag[1:-1]] = "NULL" if values[tag[1:-1]] == "" else values[tag[1:-1]]
            self.sql_cursor.execute("INSERT INTO " + self.table + " (" + ", ".join(map(lambda x: x[1:-1], self.tags)) + ") VALUES (" + ", ".join([values[tag[1:-1]] for tag in self.tags]) + ");")
            #self.sql_cursor.fetchall()

    def cleanup(self):
        self.sql_connection.close()
