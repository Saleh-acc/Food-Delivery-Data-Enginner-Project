
import yaml

class KafkaConfig():
    
        
    def __init__(self, config_path =  "/opt/spark-apps/conf/topic_mapping.yaml") :
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        print(self.config)
        
    def _get_config_json(self):
        return self.config