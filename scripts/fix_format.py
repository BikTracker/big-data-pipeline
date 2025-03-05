
lines = []

with open('/home/BigData/big-data-pipeline/output/evaluation.csv', 'r') as file:
    lines = [line.rstrip() for line in file]
    for i, line in enumerate(lines):
        if i == 0:
            lines[i] = line
        if i == 1:
            lines[i] = line.replace(',', ';', 2)
        if i == 2:
            lines[i] = line.replace(',', ';', 4)

with open ('/home/BigData/big-data-pipeline/output/evaluation.csv', 'w') as file:
    for line in lines:
        file.write(line+'\n')
