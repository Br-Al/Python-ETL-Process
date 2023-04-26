from dotenv import load_dotenv
from scripts.webtools import ETL

def main():
    load_dotenv('./.env')
    ETL()

if __name__ == '__main__':
    main()
