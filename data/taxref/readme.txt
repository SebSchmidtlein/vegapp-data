App versions starting at 4.7 (database version 8) use folder "taxref"
App versions starting at 4.5 (database version 5) use folder "taxa"
Older app versions use folder "species"

All users are >= 4.0.08: Remove index.csv
All users are >= 4.5:    Remove directory /species and versions.csv
All users are >= 4.7:    Remove directory /taxa


ids.txt is used by species_validator.html to hunt duplicates (very unlikely as we are dealing with random 18-digit numbers)

update_species_ids.py is used to update ids.txt. It reads all zips and lists the ids in the species lists
1. Open PowerShell in the d:\Git\vegapp\Vegapp Website\data\taxref\ directory
2. Type this command: python update_species_ids.py
3. Press Enter

