# formula1_databricks_demo
This is an Azure Data Bricks Demo

# Incremental Scenario

---------------------
Day 1               |
Receive All History |
(Historical File)   |
---------------------

---------------------
Day 2 + n            |
Receive New Race File|
Incremental File
---------------------

Solution Incremental Load for All files.
Incremental load needs to understan

Circuits, Races, Constructors, Drivers - Full files every refresh
Results, pitstops, laptimes, qualifying - Only new, incremental files
