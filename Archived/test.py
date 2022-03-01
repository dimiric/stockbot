# Python3 code to demonstrate 
# removing front element
# using pop(0)

qtime_list = []

# initializing list 
for value in range(0,119):
    qtime_list.append(0)
  
# Printing original list
print ("Original list is : " + str(qtime_list))
  
# using pop(0) to
# perform removal
qtime_list.pop(0)
qtime_list.append(1)
listsum = sum(qtime_list)

# Printing modified list 
print ("Modified list is : " + str(qtime_list))
print(f" sum: {listsum}")


qtime_list.pop(0)
qtime_list.append(2)
listsum = sum(qtime_list)
print ("   Final list is : " + str(qtime_list))
print(f" sum: {listsum}")
