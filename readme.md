# State Travel Hotel Stays
Data enrichment for State of Utah Travel hotel stay data.

### Project Info
Hotel stay data is provided on a quarterly basis. It is loaded as a Sheet to a shared [source data Google Drive folder](https://drive.google.com/drive/u/0/folders/1dFS89Hiwi7pK3i8YexBYpR7AKvxGGpkk).

### Run steps
1. Hotel stay sheet must be downloaded as a csv.
2. Path to stay csv is provided when running perdiem.py
3. perdiem.py will produce output csv with federal and state perdiem hotel rates added.
    * You can confirm non-Utah rates at [GSA perdiem lookup](https://www.gsa.gov/travel/plan-book/per-diem-rates/)
4. Output csv must be loaded to [results Drive folder](https://drive.google.com/drive/u/0/folders/142c6wNwX0UdFwFb7mO6kigticxzB2jyt)
    * Convert csv to Google sheet if it was not converted automatically.
5. Confirm ARRAYFORMULA's in first row have evaluated.
    * The formulas may need to be deleted from the cell and re-added to evaluate :man-shrugging:.
6. Email interested parties to notify results are ready.

### Yearly Process Update
Every new Utah fiscal year Travel produces new Utah per diem rates. They will be provided be State Travel and must replace [utah_rates.csv]
