import sys
import utils

def categorylinks_sql2csv(allCateogriesFileName, subcatsFileName, neededCategoriesFileName):
	STATE_OUTSIDE = 1
	STATE_IN_ENTRY = 2
	STATE_IN_STRING = 3
	STATE_IN_STRING_ESCAPED = 4

	neededCategories = {}
	processedLines = 0
	readBytes = 0
	isNextCharExcaped = False

	with open(allCateogriesFileName, encoding='utf8') as dataFile:
		with open(treeDumpFileName, 'a+', encoding='utf8') as outFile:
			with open(subcatsFileName, 'a+', encoding='utf8') as subcatsFile:
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

						if currentState == STATE_OUTSIDE:
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
								if parts[6] == "subcat":
									print(";".join([parts[0], parts[1]]), file=subcatsFile)
								else:
									print(";".join([parts[0], parts[1], parts[6]]), file=outFile)
								
								parts = []
								currentState = STATE_OUTSIDE
							else:
								entryBuffer += ch
						elif currentState == STATE_IN_STRING:
							if ch == "\\":
								entryBuffer += ch
								currentState = STATE_IN_STRING_ESCAPED		 
							if ch == "'":
								currentState = STATE_IN_ENTRY		 
							else:
								entryBuffer += ch
						elif currentState == STATE_IN_STRING_ESCAPED:
							entryBuffer += ch
							currentState = STATE_IN_STRING

def main():
	categorylinks_sql2csv(
		# input
		'data/enwiki-20160407-categorylinks.sql',
		
		# output
		'results/categorylinks_by_line.csv',
		'results/categorylinks_subcats_by_line.csv'
	)


if __name__ == "__main__":
	main()			
