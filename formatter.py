stuff = {}
filein = open("mbta.json")

for i in range(10):
    line = filein.readline()
    if "FIELD" in line:
        formatter = line.split(":")
        stuff[formatter[0]] = formatter[1]

filein.close()

fileout = open("mbta_cleaned.json", "w")
filein = open("mbta.json")

for line in filein:
    if "FIELD" in line:
        formatter = line.split(":")
        line = line.replace(formatter[0], stuff[formatter[0]].rstrip())
        line = "  " + line.replace(",", "", 1) 
    fileout.write(line)

fileout.close()
filein.close()

    
    
