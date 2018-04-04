data = open('data', 'r')
new_data = open('new_data', 'w')
for line in data.readlines():
    line = line.replace('(', '').replace(')', '').replace('u', '').replace('\'', '').replace(' ', '')
    new_data.write(line)