from secrets import choice
import numpy as np
from datetime import datetime, date
import pandas as pd
from pandas import json_normalize
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import configparser

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

mon_conteneur = configparser.ConfigParser()
mon_conteneur.read('config.cfg')

ChoixFichier = mon_conteneur['Data']['ChoixFichier']

if ChoixFichier == "1":
    import json
    NomJson = mon_conteneur['Data']['NomJson']
    data = open(str(NomJson)+'.json', "r")
    jsonContent = data.read()
    dict = json.loads(jsonContent)
    df2 = json_normalize(dict) 

elif ChoixFichier == "2":    
    nb_ligne = mon_conteneur['Data']['nb_ligne']
    
    def getIntRandom():
        import random
        randomlist = []
        for i in range(int(nb_ligne)):
            n = random.randint(1,10)
            randomlist.append(n)
        return randomlist

    def getFloatRandom():
        import random
        randomlist = []
        for i in range(int(nb_ligne)):
            n = np.random.random_sample()
            randomlist.append(n)
        return randomlist

    def getStringRandom():
        from random import choice
        from string import ascii_lowercase , ascii_uppercase

        chars = ascii_lowercase + ascii_uppercase
        lst = [''.join(choice(chars) for _ in range(3)) for _ in range(int(nb_ligne))]
        return lst

    def random_date():
        from random import randrange
        from datetime import timedelta
        start = datetime.strptime('1/1/2000 1:30 PM', '%m/%d/%Y %I:%M %p')
        end = datetime.strptime('1/1/2022 4:50 AM', '%m/%d/%Y %I:%M %p')
        delta = end - start
        lstdate =[] 
        for _ in range(int(nb_ligne)):
            int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
            random_second = randrange(int_delta)
            fulldate = start + timedelta(seconds=random_second)
            lstdate.append(fulldate)
        
        return lstdate

    df2 = pd.DataFrame()
    
    def getramdomcolumnInt():
        nb_colonne_int = mon_conteneur['Data']['nb_colonne_int']
        for i in range (int(nb_colonne_int)):
            df7 = pd.DataFrame(getIntRandom())
            df2['int_ID_'+ str(i) ] = df7
            
        return df2 

    def getramdomcolumnFloat():
        nb_colonne_float = mon_conteneur['Data']['nb_colonne_float']
        for i in range (int(nb_colonne_float)):
            df7 = pd.DataFrame(getFloatRandom())
            df2['float_ID_'+ str(i) ] = df7
            
        return df2 

    def getramdomcolumnString():
        nb_colonne_string = mon_conteneur['Data']['nb_colonne_string']
        for i in range (int(nb_colonne_string)):
            df7 = pd.DataFrame(getStringRandom())
            df2['string_ID_'+ str(i) ] = df7
            
        return df2  

    def getramdomcolumnDate():
        nb_colonne_date = mon_conteneur['Data']['nb_colonne_date']
        for i in range (int(nb_colonne_date)):
            df7 = pd.DataFrame(random_date())
            df2['Date_ID_'+ str(i) ] = df7
            
        return df2 


    getramdomcolumnInt()
    getramdomcolumnFloat()
    getramdomcolumnString()
    getramdomcolumnDate()

else:
    exit()

df = pd.DataFrame(df2)

choix = mon_conteneur['Data']['choix_extention']
#creation de boucle pour la generation de fichier 
folder = mon_conteneur['Data']['fichier']

NbFichier = mon_conteneur['Data']['NbFichier']

if choix == "1":
    
    for i in range (int(NbFichier)):
        from pathlib import Path  
        filepath = Path(folder + str(i)+'.csv')  
        filepath.parent.mkdir(parents=True, exist_ok=True)  
        df.to_csv(filepath)  
elif choix == "2":
    
    for i in range (int(NbFichier)):
        from pathlib import Path 
        filepath = Path(folder +str(i)+'.json')
        filepath.parent.mkdir(parents=True, exist_ok=True)  
        df.to_json(filepath)  
elif choix == "3":
    for i in range (int(NbFichier)):
        from pathlib import Path
        filepath = Path(folder +str(i)+'.parquet')
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(filepath)
elif choix == "4":
    for i in range (int(NbFichier)):
        from pathlib import Path
        filepath = Path(folder +str(i)+'.xml')
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df.to_xml(filepath)
elif choix == "5":
    for i in range (int(NbFichier)):
        from pathlib import Path
        filepath = Path(folder +str(i)+'.orc')
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df.to_orc(filepath)
elif choix == "6":
    exit()



exit()

