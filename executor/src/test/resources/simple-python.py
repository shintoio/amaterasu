data = [1, 2, 3, 4, 5]
print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print(data)

with open('/Users/roadan/in.txt', 'a') as the_file:
    the_file.write('hi there\n') # python will convert \n to os.linesep
