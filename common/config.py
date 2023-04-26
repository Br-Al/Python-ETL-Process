import yaml
import json

PATH_TO_DB_CONFIG_FILE = "C:/config.yaml"


def get_config():
    with open(PATH_TO_DB_CONFIG_FILE, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def get_api_config(my_api_path):
    try:
        with open(my_api_path, 'r') as f:
            api_cred = json.load(f)
            return api_cred
    except Exception as e:
        print(e)


