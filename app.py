# Importing libraries
from dotenv import load_dotenv

load_dotenv(".env", verbose=True)

if __name__ == '__main__':
    from src.main.python.main import *

    app()
