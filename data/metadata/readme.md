# Table Metadata
The .csv Data Dictionary files were manually downloaded from FiscalData.  (They are accessible via Button Push -- API would be better!)  

The .csv DD files are converted to RAML using the export_fddatadict_to_raml() function from [dd_functions.py](python/dd_functions.py) common module.
See https://github.com/froghollow/data-dictionary-workbook for details.

```python
## export a FiscalData CSV Data Dictionary file to a RAML Data Type
import sys
if './python' not in sys.path: sys.path.append('./python')

from dd_functions import export_fddatadict_to_raml
#help(export_fddatadict_to_raml)

dd_files = [
    "metadata/Average Interest Rates on US Treasury Securities Data Dictionary.csv",
    "metadata/Treasury Offset Program Data Dictionary.csv"  # both Federal and State are in this file
]
for csv_input_path in dd_files:
    raml_output_path = csv_input_path.replace('.csv','.raml').replace(' ','_').lower()

    export_fddatadict_to_raml( csv_input_path, raml_output_path )
```

RAML file names have been manually edited to be consistent with data file names downloaded via REST API

If we store RAML Data Type fragments are on our Enterprise Anypoint Exchange, then we can retreive table metadata from there via REST API.
