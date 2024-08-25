# CSVDatasetJoinOperator
Class that implements join operations for two input csv datasets.

CSV Join Operation

This class implements a common join operation between two csv datasets 
on a user specified key. It implements all join types: inner, left, right, outer.  If no join type is specified it defaults to inner.

Note: It does NOT use any dependent liraries like pandas.

I was asked to implement a similar python class many years ago during a
live coding interview - and vowed to re-implement my answer and share on
github.
   
Author: Anthony Asta

Copyright: Copyright 2023, Anthony Asta

------------------------------------------------


Example Usage (from command line):
```
% python3 ./CSVDatasetJoinOperator.py
Enter the path to the first CSV file: ./CSV1.csv
Enter the path to the second CSV file: ./CSV2.csv
Enter the field to join on (column name): name
Enter the join type (inner, outer, left, right): inner
Enter the output CSV file path: ./CSVJoined.csv
Joined data saved to csv ./CSVJoined.csv
```



