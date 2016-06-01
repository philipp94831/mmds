import csv

def categorylinks_name2id(subcatsFileName, categoriesFileName, outputFileName):
	# Categories indexed by name
	categories = {}

	with open(categoriesFileName, encoding='utf8') as categoriesFile:
		for line in categoriesFile:
			#parts = csv.reader(line, delimiter=";")
			parts = line.split(';')
			print(parts)
			categories[parts[1]] = parts[0]			 

#	with open(subcatsFileName, encoding='utf8') as subcatsFile:
#		with open(outputFileName, 'a+', encoding='utf8') as outputFile:
#			for line in subcatsFile:
#				parts = csv.reader(line, delimiter=";")
#				parentId = parts[0]
#				if not categories[parts[1]]:
#					raise ValueError("Category with name: '" + parts[1] + "' not found!") 
#				childId = categories[parts[1]]
#
#				csv.writer(outputFile, [parentId, childId], delimiter=";");



def main():
	categorylinks_name2id(
		# input
		'data/categorylinks_subcats_by_line.csv',
		'data/categories_by_line.csv',		

		# output
		'data/categorylinks_subcats_ids.csv',
	)

if __name__ == "__main__":
	main()	