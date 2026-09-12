
import yaml

class PostgresConfig():
    
        
    def __init__(self, config_path =  "/opt/spark-apps/conf/batches_mapping.yaml") :
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        print(self.config)
        
    def _get_config_json(self):
        return self.config