def export_fddatadict_to_raml( csv_input_path, raml_output_path='' ):
    ''' Export FiscalData Data Dictionary CSV file to RAML v1.0 DataType '''
    import csv
    import json
    import yaml

    #data = {}
    raml_types = {}

    # Open a csv reader called DictReader
    with open(csv_input_path, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)

        # Convert each row into a dictionary and add it to RAML output
        for row in csvReader:
            dataset = row['dataset']
            data_table_name = row['data_table_name']
            field_name = row['field_name']

            # convert dictionary data type to RAML data type ...
            # (just using scalar types for simplicity -- RAML also supports 'custom' object types that can define discrete charactistics)
            data_type = row['data_type'].lower()
            if data_type in ('currency','percentage'):
                data_type = 'number'
            elif data_type in ('year','quarter','month','day'):
                data_type = 'int'
            elif data_type == 'date':
                data_type = 'date'
            elif data_type == 'timestamp':
                data_type = 'timestamp'
            else:
                data_type = 'string'

            if data_table_name not in raml_types.keys():
                raml_types[data_table_name] = {
                    'type' : 'object',
                    'properties' : {} }
            raml_types[data_table_name]['properties'][field_name] = {
                'type' : data_type,
                'displayName' : row['display_name'],
                'description' : row['description'],
                'required' : row['is_required'] == '1',
                '(fd_data_type)' : row['data_type']
            }
                
    yaml_template = f"#%RAML 1.0\n\ntitle: Import from FiscalData Data Dictionary {dataset} Dataset\n"
    raml_types = {"types" : raml_types}
    raml = yaml_template + yaml.dump(raml_types, sort_keys=False)

    if raml_output_path > '':
        with open( raml_output_path, 'w') as f:
            resp = f.write(raml)
        f.close()
        print( f"Output to file '{raml_output_path}'" )
    else:
        print( raml)

    return 


def export_fdmeta_to_raml( json_meta, raml_output_path='' ):
    ''' export FiscalData Meta Object (from API Response) to RAML v1.0 DataType '''
    import yaml

    raml_types = {}
    for key in json_meta['labels'].keys():
        #print(key)
        raml_types[key] = {
                'type' : json_meta['dataTypes'][key].lower() ,
                'displayName' : json_meta['labels'][key] ,
                'format' : json_meta['dataFormats'][key]
            }
    yaml_template = f"#%RAML 1.0\n\ntitle: Import from FiscalData Meta Object\n"
    raml_types = {"types" : raml_types}
    raml = yaml_template + yaml.dump(raml_types, sort_keys=False)

    if raml_output_path > '':
        with open( raml_output_path, 'w') as f:
            resp = f.write(raml)
        f.close()
        print( f"Output to file '{raml_output_path}'" )
    else:
        print( raml)

    return

