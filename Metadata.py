#!/usr/bin/python3
# Metadata.py
# Ashish D'Souza (computer_geek64 or computer-geek64)
# June 9th, 2019

import os
import mysql.connector as sql
from getpass import getpass as password


class Metadata:
    def __init__(self, directory: str, tags: tuple, **kwargs):
        self.directory = directory
        self.tags = tuple(["[" + tag + "]" for tag in tags])
        self.user, self.host = input("user@host >> ").split("@")
        self.password = password(self.user + "@" + self.host + "'s password: ")
        if "database" in kwargs.keys():
            self.sql_database = kwargs["database"]
            self.sql_connection = sql.connect(host=self.host, user=self.user, password=self.password, database=self.sql_database)
        else:
            self.sql_connection = sql.connect(host=self.host, user=self.user, password=self.password)
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

    def update_database(self):
        self.sql_cursor.execute("")

    def cleanup(self):
        self.sql_connection.close()
