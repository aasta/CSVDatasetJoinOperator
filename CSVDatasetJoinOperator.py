#!/usr/bin/env python3

"""CSV Join Operation

   Thie class implements a common join operation between two csv datasets
   on a user specified field. It implements all join types:
   inner, left, right, outer.  If no join type is specified it defaults to inner.

   Note: It does NOT use any dependent liraries like pandas.

   I was asked to implement a similar python class many years ago during a
   live coding interview - and vowed to re-implement my answer and share on
   github.

   What is the time complexity of my join algorithm?
   
   Author: Anthony Asta
   Copyright: Copyright 2023, Anthony Asta

"""

__author__ = "Anthony Asta"
__copyright__ = "Copyright 2023, Anthony Asta"

__license__ = "Apache License 2.0"
__version__ = "1.01"
__maintainer__ = "Anthony Asta"
__email__ = "asta151-second@yahoo.com"
__status__ = "Production"

import csv

class CSVDatasetJoinOperator(object):
    """
    CSVDatasetJoinOperator

    Class that implements join operations for two input csv datasets.

    Attributes:
        csvfile1: First csv dataset to be joined.
        csvfile2: Second csv dataset to be joined.
        joinfield: Common filed (by name) used too join both csv datasets
        join_type: inner, left, right, outer
    """
    def __init__(self, csvfile1, csvfile2, join_field, join_type='inner'):
        self.csvfile1 = csvfile1
        self.csvfile2 = csvfile2
        self.join_field = join_field
        self.join_type = join_type.lower()
        self.data1 = None
        self.data2 = None
        self.joined_data = None

    def load_csv(self, file_path):
        """Loads a CSV file into a list of dictionaries."""
        try:
            with open(file_path, mode='r') as file:
                reader = csv.DictReader(file)
                return list(reader)
        except FileNotFoundError:
            print(f"Error: The file {file_path} was not found.")
            return None

    def join_csv(self):
        """Joins two CSV files as lists of dictionaries on the specified field."""
        joined_data = []
        
        if self.join_field not in self.data1[0] or self.join_field not in self.data2[0]:
            print(f"Error: The field '{self.join_field}' does not exist in both files.")
            return None

        # Convert data2 into a dictionary for fast lookup based on the join_field
        data2_dict = {row[self.join_field]: row for row in self.data2}

        if self.join_type == 'inner':
            for row1 in self.data1:
                if row1[self.join_field] in data2_dict:
                    joined_row = {**row1, **data2_dict[row1[self.join_field]]}
                    joined_data.append(joined_row)
        elif self.join_type == 'left':
            for row1 in self.data1:
                if row1[self.join_field] in data2_dict:
                    joined_row = {**row1, **data2_dict[row1[self.join_field]]}
                else:
                    joined_row = row1.copy()
                joined_data.append(joined_row)
        elif self.join_type == 'right':
            for row2 in self.data2:
                if row2[self.join_field] in data2_dict:
                    joined_row = {**data2_dict[row2[self.join_field]], **row2}
                else:
                    joined_row = row2.copy()
                joined_data.append(joined_row)
        elif self.join_type == 'outer':
            matched_keys = set()

            # Perform left join part
            for row1 in self.data1:
                if row1[self.join_field] in data2_dict:
                    joined_row = {**row1, **data2_dict[row1[self.join_field]]}
                    matched_keys.add(row1[self.join_field])
                else:
                    joined_row = row1.copy()
                joined_data.append(joined_row)

            # Add rows from data2 that didn't match any in data1
            for row2 in self.data2:
                if row2[self.join_field] not in matched_keys:
                    joined_data.append(row2)

        self.joined_data = joined_data

    def save_csv(self, output_file):
        """Writes the joined data to a CSV file."""
        if self.joined_data is not None:
            # prepare sorted fieldnames for writing to the output csv file
            all_fieldnames = list({key for row in self.joined_data for key in row.keys()})
            all_fieldnames.sort() # sort the output field names alphabetically

            # write output data to csv file - note this overwrites file if it exists
            with open(output_file, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=all_fieldnames)
                writer.writeheader()
                writer.writerows(self.joined_data)
            print(f"Joined data saved to csv {output_file}")
        else:
            print("No joined data to save.")

    def execute_join(self):
        """Executes the CSV join operation."""
        # Load CSV files
        self.data1 = self.load_csv(self.csvfile1)
        self.data2 = self.load_csv(self.csvfile2)

        if self.data1 is None or self.data2 is None:
            print("Could not load files. Exiting.")
            return

        # Perform the CSV join
        self.join_csv()


if __name__ == "__main__":
    # Example usage
    csvfile1 = input("Enter the path to the first CSV file: ")
    csvfile2 = input("Enter the path to the second CSV file: ")
    join_field = input("Enter the field to join on (column name): ")
    join_type = input("Enter the join type (inner, outer, left, right): ").lower()

    joiner = CSVDatasetJoinOperator(csvfile1, csvfile2, join_field, join_type)
    joiner.execute_join()

    output_file = input("Enter the output CSV file path: ")
    joiner.save_csv(output_file)
