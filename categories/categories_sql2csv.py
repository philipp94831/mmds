import sys

def categories_sql2csv(allCateogriesFileName, byLineFileName):
	STATE_OUTSIDE = 1
	STATE_IN_ENTRY = 2
	STATE_IN_STRING = 3
	STATE_IN_STRING_ESCAPED = 4

	neededCategories = {}

	#with open(neededCategoriesFileName) as categoriesFile:
	#	for line in categoriesFile:
	#		categoryName = line[:line.rfind(";")]
	#		neededCategories[categoryName] = True 


	processedLines = 0
	readBytes = 0
	isNextCharExcaped = False
	hasEscapedSequence = False

	with open(allCateogriesFileName, encoding='latin-1') as dataFile:
		with open(byLineFileName, 'a+', encoding='latin-1') as outFile:		
			while True:
				# jump to INSERT INTO
				expectedBeginning = "INSERT INTO"
				while True:
					lineBeginning = dataFile.read(len(expectedBeginning))
					#print (lineBeginning)
					if lineBeginning == "" or lineBeginning == expectedBeginning:
						break
					dataFile.readline() # read and throw rest of the line

				# we are now in expected line
				currentState = STATE_OUTSIDE
				entryBuffer = ""
				parts = []

				while True:
					ch = dataFile.read(1)
					readBytes += 1
					if ch == "":
						print ("EOF")
						return
					elif currentState == STATE_OUTSIDE:
						entryBuffer = ""
						if ch == "\n":
							processedLines += 1
							print (str(processedLines) + ". line processed, " + sizeof_fmt(readBytes) + " read")						
						elif ch == "(":
							currentState = STATE_IN_ENTRY
					elif currentState == STATE_IN_ENTRY:
						if ch == "'":
							currentState = STATE_IN_STRING
						elif ch == ",":
							parts.append(entryBuffer)
							entryBuffer = ""
						elif ch == ")":
							parts.append(entryBuffer)
							entryBuffer = ""
							
							# PRINT
							if hasEscapedSequence:
								print (parts)
							print(";".join(parts), file=outFile)
							
							parts = []
							hasEscapedSequence = False
							currentState = STATE_OUTSIDE
						else:
							entryBuffer += ch
					elif currentState == STATE_IN_STRING:
						if ch == "\\":
							print ("\tCHAR AT DET: " + ch)
							currentState = STATE_IN_STRING_ESCAPED		 
						if ch == "'":
							currentState = STATE_IN_ENTRY		 
						else:
							entryBuffer += ch
					elif currentState == STATE_IN_STRING_ESCAPED:
						print ("\tCHAR AFTER : " + ch)
						entryBuffer += ch
						hasEscapedSequence = True
						currentState = STATE_IN_STRING

def main():
	categories_sql2csv(
		# input 
		'edits/enwiki-20160407-category.sql',

		# output
		'results/categories_by_line.csv'
	)


#########
# UTILS #
#########
def uprint(*objects, sep=' ', end='\n', file=sys.stdout):
    enc = file.encoding
    if enc == 'UTF-8':
        print(*objects, sep=sep, end=end, file=file)
    else:
        f = lambda obj: str(obj).encode(enc, errors='backslashreplace').decode(enc)
        print(*map(f, objects), sep=sep, end=end, file=file)

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)           	

if __name__ == "__main__":
	main()			
