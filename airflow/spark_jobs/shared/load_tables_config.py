
import json



def load_tables_config(tables_arg, json_path):
    with open(json_path, "r") as f:
            tables = json.load(f)
            
    list_tables = tables_arg if isinstance(tables_arg, list) else [tables_arg]
    print(list_tables)
    running_tables = {
                        name: tables[name]
                        for name in list_tables
                            }
    return running_tables