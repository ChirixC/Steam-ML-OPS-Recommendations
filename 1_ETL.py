import pandas as pd
import numpy as np
from nltk.sentiment.vader import SentimentIntensityAnalyzer

''' Antes de comenzar nuestro ETL, se hizo un previo procesado de las bases de datos originales con la herramiente "codebeautyfy.org/json-fixer"
### Para así obtener archivos en formato json váldos.'''

# Constante para la ruta de nuestros archivos [0] = Games [1] = reviews [2] = items
FILES_PATH = ['.\\Datasets\\steam_games.json.gz','.\\Datasets\\user_reviews_fixed.json.gz','.\\Datasets\\users_items_fixed.json.gz']

def load_files(file_path):
    return True