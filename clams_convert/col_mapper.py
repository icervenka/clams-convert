import pandas as pd
import os


class ColMapper:

    def __init__(self, system, file=None):
        self.system = system
        self.specs = None
        self.mapper = None
        self.finder = None
        self.typer = None
        self.no_params = 0

        self.read_specs(file).create()

    def read_specs(self, file):
        if file is None:
            self.specs = pd.read_csv("specs/"+self.system+".txt", sep='\t')
        else:
            if os.path.exists(file):
                self.specs = pd.read_csv(file, sep='\t', na_values=['None'])
            else:
                raise ValueError("No specification file for columns was found")
        return self

    def create(self):
        try:
            self.mapper = self.specs.set_index('colnames').loc[:, 'app'].dropna().to_dict()
            self.finder = self.specs.set_index('app').loc[:, 'colnames'].dropna().to_dict()
            self.typer = self.specs.set_index('colnames').loc[:, 'type'].dropna().to_dict()
            self.no_params = len(self.specs["aggregate"].dropna())
        except KeyError:
            print("Incorrect column names in specification file")
            raise

    def find(self, name):
        return self.finder[name]

    def update(self, spec, pos):
        line = pd.DataFrame(spec, index=[pos+0.5])
        self.specs = self.specs.append(line, ignore_index=False)
        self.specs = self.specs.sort_index().reset_index(drop=True)
        self.create()

    def validate(self, file):
        #TODO check if all required columns are present after prettify
        pass

    def __str__(self):
        print(vars(self))
