'''
Authors: James Otis, Max Biel
File type reading wrappers for CS591 Data mechanics with Andrei Lapets
'''

import csv
import xlrd


class CSV:
    def __init__(self, filePath):
        self.filePath = filePath
        self.keys = []
        try:
            open(filePath, 'rb')
            if filePath[-3:].lower() != 'csv':
                raise Exception('Invalid Filetype for CSV')
        except Exception:
            raise Exception('No file found at path')

    def setKeys(self):
        '''
        sets self.keys to contain the keys in the CSV
        '''
        with open(self.filePath, 'r', newline='', encoding='mac_roman') as csvfile:
            reader = csv.reader(csvfile)
            setKeys = False
            for row in reader:
                if setKeys == False:
                    self.keys += ''.join(row).split('_')
                    setKeys = True

    def readIntoDict(self):
        '''
        Returns an array of list Items that contain a single piece of data.
        '''
        resultant = []
        with open(self.filePath, 'r', newline='', encoding='mac_roman') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                resultant.append(row)
            return resultant


class excel:
    def __init__(self, filePath):
        self.filePath = filePath
        self.keys = []
        try:
            open(filePath, 'rb')
        except Exception:
            raise Exception('No file could be found for the specified path')

    def setKeys(self):
        '''
        returns the column names at the top of a spreadsheet
        '''
        workbook = xlrd.open_workbook(self.filePath)
        first_sheet = workbook.sheet_by_index(0)
        for i in first_sheet.row(0):
            self.keys += [i.value]


    def readIntoDict(self):
        '''
        returns a list of dictionaries that correspond to the column names of an excel worksheet
        '''
        resultant = []
        workbook = xlrd.open_workbook(self.filePath)
        for i in range(1, workbook.sheet_by_index(0).nrows):
            for j in range(len(self.keys) - 1):
                resultant.append({self.keys[j]: workbook.sheet_by_index(0).row(i)[j].value, self.keys[j + 1]: workbook.sheet_by_index(0).row(i)[j + 1].value})
        return resultant





testSheet = CSV('/Users/jmbiel/Downloads/Vegetables.csv')
testSheet.setKeys()
fileobj = open("/Users/jmbiel/Desktop/food_prices.json", "w+")
fileobj.write(str(testSheet.readIntoDict()))
fileobj.close()


#testCSV = CSV('crime.csv')
#testCSV.setKeys()
#print(testCSV.keys)
# print(testCSV.readIntoDict())
