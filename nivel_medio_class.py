# python .\nivel_medio_class.py .\DataBase\wowah_data.csv  > .\resultado.\nivel.txt
from mrjob.job import MRJob
import numpy as np

class NivelMediaClass(MRJob):

    def mapper(self, _ , line):
        try:
            coluns = line.split(",")
            nivel = int(coluns[1].strip())
            clase = coluns[3].strip() 
            yield clase ,(nivel, 1)
        except ValueError:
            pass 
        
    def reducer(self, key, values):
        total_nivel = 0
        total_count = 0
        min_nivel = np.inf
        max_nivel = -np.inf

        for nivel, count in values:
            total_nivel += nivel
            total_count += count
            min_nivel = min(min_nivel, nivel)
            max_nivel = max(max_nivel, nivel)

        media = int(total_nivel / total_count) if total_count > 0 else 0
        yield key, {"media": media, "min": min_nivel, "max": max_nivel}
        

if __name__ == "__main__":
    NivelMediaClass.run()