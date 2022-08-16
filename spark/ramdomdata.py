from secrets import choice
import numpy as np
from datetime import datetime, date
import pandas as pd
from pandas import json_normalize
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

ChoixFichier = int(input("veuillez entrer le choix  entre 1/ Importer un fichier Json ou 2/ Cree un dataFrame random "))

if ChoixFichier == 1:
    import json

    data = open("data-CqaOuDi6BIiK0XPsVYhfZ.json", "r")
    jsonContent = data.read()
    dict = json.loads(jsonContent)
    print(dict)
    df2 = json_normalize(dict) 
    print(df2)

elif ChoixFichier == 2:    

    nb_ligne = int(input("veuillez entrer le nombre de ligne "))

    def getIntRandom():
        import random
        randomlist = []
        for i in range(nb_ligne):
            n = random.randint(1,10)
            randomlist.append(n)
        return randomlist

    def getFloatRandom():
        import random
        randomlist = []
        for i in range(nb_ligne):
            n = np.random.random_sample()
            randomlist.append(n)
        return randomlist

    def getStringRandom():
        from random import choice
        from string import ascii_lowercase , ascii_uppercase

        chars = ascii_lowercase + ascii_uppercase
        lst = [''.join(choice(chars) for _ in range(3)) for _ in range(nb_ligne)]
        return lst

    def random_date():
        from random import randrange
        from datetime import timedelta
        start = datetime.strptime('1/1/2000 1:30 PM', '%m/%d/%Y %I:%M %p')
        end = datetime.strptime('1/1/2022 4:50 AM', '%m/%d/%Y %I:%M %p')
        delta = end - start
        lstdate =[] 
        for _ in range(nb_ligne):
            int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
            random_second = randrange(int_delta)
            fulldate = start + timedelta(seconds=random_second)
            lstdate.append(fulldate)
        
        return lstdate

    df2 = pd.DataFrame()

    namecolumn = 'A'

    nb_colonne_int = int(input("veuillez entrer le nombre de colone de valeur int "))
    nb_colonne_float = int(input("veuillez entrer le nombre de colone de valeur float "))
    nb_colonne_string = int(input("veuillez entrer le nombre de colone de valeur string "))
    nb_colonne_date = int(input("veuillez entrer le nombre de colone de valeur date "))



    def getramdomcolumnInt(nb_colonne_int):
        
        for i in range (nb_colonne_int):
            df7 = pd.DataFrame(getIntRandom())
            df2['int_ID_'+ str(i) ] = df7
            
        return df2 

    def getramdomcolumnFloat(nb_colonne_float):
        
        for i in range (nb_colonne_float):
            df7 = pd.DataFrame(getFloatRandom())
            df2['float_ID_'+ str(i) ] = df7
            
        return df2 

    def getramdomcolumnString(nb_colonne_string):
        
        for i in range (nb_colonne_string):
            df7 = pd.DataFrame(getStringRandom())
            df2['string_ID_'+ str(i) ] = df7
            
        return df2  

    def getramdomcolumnDate(nb_colonne_date):
        
        for i in range (nb_colonne_date):
            df7 = pd.DataFrame(random_date())
            df2['Date_ID_'+ str(i) ] = df7
            
        return df2 


    getramdomcolumnInt(nb_colonne_int)
    getramdomcolumnFloat(nb_colonne_float)
    getramdomcolumnString(nb_colonne_string)
    getramdomcolumnDate(nb_colonne_date)

else:
    exit()

df = pd.DataFrame(df2)

print ("1 pour fichier CSV / 2 pour un fichier json ")
print ("3 pour fichier Parquet / 4 pour un fichier XML ")
print ("5 pour fichier ORC / 6 pour exit")
choix = int(input("veuillez entrer un choix "))


x = 2 
if choix == 1:
    from pathlib import Path  
    filepath = Path('folder/subfolder/data-'+ str(x) +'.csv')  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    df.to_csv(filepath)  
elif choix == 2:
    from pathlib import Path  
    filepath = Path('folder/subfolder/data.json')
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    df.to_json(filepath)  
elif choix == 3:
    from pathlib import Path
    filepath = Path('folder/subfolder/data.parquet')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(filepath)
elif choix == 4:
    from pathlib import Path
    filepath = Path('folder/subfolder/data.xml')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_xml(filepath)
elif choix == 5:
    from pathlib import Path
    filepath = Path('folder/subfolder/data.orc')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_orc(filepath)
elif choix == 6:
    exit()



exit()

