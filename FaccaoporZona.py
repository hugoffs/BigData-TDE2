# python .\FaccaoporZona.py .\wowah_data.csv > .\resultado.\faccao_por_zona.txt
from mrjob.job import MRJob
from collections import Counter

class FaccaoPorZona(MRJob):

    def mapper(self, _, line):
        try:
            cols = line.strip().split(',')
            if len(cols) < 6:
                return
            zona = cols[4].strip()
            raca = cols[2].strip().capitalize()

            alliance = {'Human', 'Dwarf', 'Night Elf', 'Gnome', 'Draenei', 'Worgen'}
            horde = {'Orc', 'Undead', 'Tauren', 'Troll', 'Blood Elf', 'Goblin'}

            if raca in alliance:
                faccao = 'Alliance'
            elif raca in horde:
                faccao = 'Horde'
            else:
                faccao = 'Unknown'

            yield zona, faccao

        except Exception:
            pass

    def reducer(self, zona, faccoes):
        contador = Counter(faccoes)
        yield zona, dict(contador)

if __name__ == "__main__":
    FaccaoPorZona.run()
