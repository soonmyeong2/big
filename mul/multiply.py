import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	key = record[0]
	
	for i in range(5) :
		if(key == 'a'):
			mr.emit_intermediate((record[1], i), (record[2], record[3]))
		else :
			mr.emit_intermediate((i, record[2]), (record[1], record[3]))

def reducer(key, list_of_values):
	value = 0
	ary1 = {}
	ary2 = {}

	for i in (list_of_values) :
		if i[0] in ary1 :
			ary2[i[0]] = i[1]
		else :
			ary1[i[0]] = i[1]
	for j in ary2.keys() :
		value += ary1[j] * ary2[j]

	mr.emit((key[0],key[1],value))
		

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
