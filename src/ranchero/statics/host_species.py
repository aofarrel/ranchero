from numpy import nan as nan

# key: what SRA says
# value: [scientific name, scientific name certainty (1-3, 3 is highest), common name]

exact_match_only = {
	# CASE SENSITIVE!!!
	'9606': ['Homo sapiens', 3, "human"], # NCBI taxid 9606 is human
	'9606, Homo sapiens': ['Homo sapiens', 3, "human"], # NCBI taxid 9606 is human
	'antelope': [None, 3, "antelope"],
	'Buffalo': [None, 3, "buffalo"],    # can't wash your hands
	'Bison': ['Bison sp.', 2, "bison"], # can wash your hands
	'Canis': ['Canis sp.', 3, "canine"],
	'DEER': [None, 3, "deer"],
	'Deer': [None, 3, "deer"],
	'deer': [None, 3, "deer"],
	'dog': ['Canis lupus familiaris', 2, "domestic dog"],
	'Elk': [None, 3, "elk"],
	'elk': [None, 3, "elk"],
	'Elephant': [None, 3, "elephant"],
}

species = {
	# NOT case sensitive
	'African Elephant': ['Loxodonta sp.', 3, "African elephant"],
	'African clawed toad': ['Xenopus laevis', 3, 'African clawed frog'],
	'Antelope Cervicapra': ['Antilope cervicapra', 3, "blackbuck"],
	'/^antelope zoo$/': [None, 3, "antelope"],
	'/^antilope zoo$/': [None, 3, "antelope"],
	'Axis axis': ['Axis axis', 3, "chital deer"],
	'Acipenser baerii': ['Acipenser baerii', 3, 'Siberian sturgeon'],
	'Acanthamoeba': ['Acanthamoeba sp.', 2, "acanthamoeba"],
	'Argyrosomus regius': ['Argyrosomus regius', 3, "meagre"],

	'BADGER': ['Meles meles', 1, "badger"],
	
	'Bison bison athabascae': ['Bison bison athabascae', 3, "wood bison"],
	'Bison bonasus': ['Bison bonasus', 3, "European bison"],
	'Bobcat': ['Lynx rufus', 2, "bobcat"],
	'Bos indicus': ['Bos indicus', 3, "zebu"],
	'Bos gaurus': ['Bos gaurus', 3, "gaur"],
	'Bos taurus': ['Bos taurus', 3, "domestic cattle"],
	'bovine': ['Bos taurus', 2, "bovine"],
	'Bubalus bubalis': ['Bubalus bubalis', 3, "water buffalo"],
	
	'canid': ['Canis sp.', 2, "canine"], # canine = canid
	'canine': ['Canis sp.', 2, "canine"],
	'Canis lupus familiaris': ['Canis lupus familiaris', 3, "domestic dog"],
	'Capra hircus': ['Capra hircus', 3, "domestic goat"],
	'Capreolus capreolus': ['Capreolus capreolus', 3, "roe deer"],
	'cattle': ['Bos taurus', 2, "domestic cattle"],
	'Cervid': [None, 2, "deer"],
	'Cervine': [None, 2, "deer"],
	'Cervus elaphus': ['Cervus elaphus', 3, "red deer"],
	'Cow?': ['Bos taurus', 2, "domestic cattle"],
	'cow': ['Bos taurus', 2, "domestic cattle"],
	'Coyote': ['Canis latrans', 3, "coyote"],
	'Chimpanzee': ['Pan troglodytes', 3, "chimpanzee"],
	
	'Dama dama': ['Dama dama', 3, "fallow deer"],
	'Dassie': ['Procavia capensis', 3, "dassie"],
	'dairy cow': ['Bos taurus', 2, "domestic cattle"],
	'dairy herd': ['Bos taurus', 2, "domestic cattle"],
	'dairy': ['Bos taurus', 2, "domestic cattle"],
	'Dasypus novemcinctus': ['Dasypus novemcinctus', 3, "nine-banded armadillo"],
	'Deer Unknown': [None, 3, "deer"],
	'Didelphis virginiana': ['Didelphis virginiana', 3, "Virgina opossum"],
	'Dicentrarchus labra': ['Dicentrarchus labra', 3, "European seabass"],
	
	'Elephas maximus': ['Elephas maximus', 3, "Asian elephant"],
	'Environment': [None, None, None],
	'Eulemur fulvus': ['Eulemur fulvus', 3, "common brown lemur"],
	
	'Feline': ['Felis catus', 1, "domestic cat"], # likely domestic cats since it's just NZ so far
	'Felis catus': ['Felis catus', 3, "domestic cat"],
	'FERRET': ['Mustela sp.', 2, "ferret"], # could be domestic, could be black-footed
	'Fox': [None, 3, "fox"],
	
	'Goat': ['Capra hircus', 3, "domestic goat"],
	'Gymnothorax funebris': ['Gymnothorax funebris', 3, "green moray"],
	
	'Hedgehog': [None, 3, "hedgehog"],
	'Helarctos malayanus': ['Helarctos malayanus', 3, "sun bear"],
	'Homosapian': ['Homo sapiens', 3, "human"],
	'Homo-sapien': ['Homo sapiens', 3, "human"],
	'homo sapien': ['Homo sapiens', 3, "human"],
	'Homo sapiens': ['Homo sapiens', 3, "human"],
	'Homo sapiens NCBI:': ['Homo sapiens', 3, "human"],
	'Homo sapiens NCBI:9606': ['Homo sapiens', 3, "human"],
	'Homo sapiens sapiens': ['Homo sapiens', 3, "human"],
	'Homo sapiens human': ['Homo sapiens', 3, "human"],
	'Humaine': ['Homo sapiens', 3, "human"], # French translation
	'Humain': ['Homo sapiens', 3, "human"],
	'Human, Homo sapiens': ['Homo sapiens', 3, "human"],
	'Human': ['Homo sapiens', 3, "human"],
	'Human host': ['Homo sapiens', 3, "human"],
	'human patient': ['Homo sapiens', 3, "human"],
	'Human/Culture': ['Homo sapiens', 1, "human"], # low-confidence since could be "human OR culture"
	'Hydrochoerus hydrochaeris': ['Hydrochoerus hydrochaeris', 3, "capybara"],
	'Hydrochoerus hydrochaeris capybara': ['Hydrochoerus hydrochaeris', 3, "capybara"],
	'Hynobius hidamontanus': ['Hynobius hidamontanus', 3, "Hakuba salamander"],
	'Hirundichthys oxycephalus': ['Hirundichthys oxycephalus', 3, "bony flyingfish"],
	
	'Jaguar': ['Panthera onca', 3, "jaguar"],

	'Kobus ellipsiprymnus': ['Kobus ellipsiprymnus', 3, "waterbuck"],
	'Kobus ellipsiprymnus waterbuck': ['Kobus ellipsiprymnus', 3, "waterbuck"],
	
	'Lama glama': ['Lama glama', 3, "llama"],
	'Locustana pardilana': ['Locustana pardilana', 3, "brown locust"],
	'Loxodonta': ['Loxodonta sp.', 2,"African elephant"],
	'Lutra lutra': ['Lutra lutra', 3, "Eurasian otter"],
	'Lycaon pictus': ['Lycaon pictus', 3, "African wild dog"],
	
	'Macaca fascicularis': ['Macaca fascicularis', 3, "crab-eating macaque"],
	'Macaca mulatta': ['Macaca mulatta', 3, "rhesus macaque"],
	'Meles meles': ['Meles meles', 3, "European badger"],
	'mouse': ['Mus musculus', 2, "mouse"],
	'Mungos mungo': ['Mungos mungo', 3, "banded mongoose"],
	'Monkey': [None, 2, "monkey"],
	'Monkey, Cynomolgus': ['Macaca fascicularis', 2, "crab-eating macaque"],
	'Moufflon': ['Ovis gmelini', 3, "moufflon"],
	'Mus musculus': ['Mus musculus', 3, "mouse"],
	'Mus musculus mouse': ['Mus musculus', 3, "mouse"],
	'Mus musculus infection model': ['Mus musculus', 3, "mouse"],
	'Mustela sp.': ['Mustela sp.', 2, "ferret"],
	'Mustela sp. ferret': ['Mustela sp.', 2, "ferret"],
	'Mustela erminea': ['Mustela erminea', 3, "stoat"],
	'Mycobacterium tuberculosis': [None, None, None],
	'Morone saxatilis': ['Morone saxatilis', 3, "striped bass"],
	
	'NAN': [None, None, None],
	'None': [None, None, None],
	'nine banded armadillo': ['Dasypus novemcinctus', 3, "nine-banded armadillo"],
	'non human primate': [None, 2, "non-human primate"],
	'non-human primate': [None, 2, "non-human primate"],
	'NHP': [None, 1, "non-human primate"], # see https://www.cdc.gov/mmwr/volumes/73/wr/mm7307a2.htm
	
	'Opossum': ['Didelphis virginiana', 1, "opossum"], # there are other species but they usually aren't just called "opossum"
	'Otaria byronia': ['Otaria byronia', 3, "South American sea lion"],
	'Oryx zoo': ['Oryx sp.', 2, "oryx"],
	'Ovis aries': ['Ovis aries', 3, "domestic sheep"],
	'Ovis aries domestic sheep': ['Ovis aries', 3, "domestic sheep"],
	'Otaria flavescens': ['Otaria flavescens', 3, 'South American sea lion'],
	
	'patient': ['Homo sapiens', 1, "human"], # veterinary medicine sometimes uses "patient"

	'Pan troglodytes': ['Pan troglodytes', 3, "chimpanzee"],
	'Panicum virgatum L. cultivar Cave-in-rock': ['Panicum virgatum L. cultivar Cave-in-rock', 3, 'Cave-in-Rock switchgrass'],
	'Panthera leo': ['Panthera leo', 3, "lion"],
	'Panthera onca': ['Panthera onca', 3, "jaguar"],
	'Panthera pardus': ['Panthera pardus', 3, "leopard"],
	'Papio sp.': ['Papio sp.', 2, "baboon"],
	'PORCINE': ['Sus scrofa', 1, "wild boar or pig"],
	'Pig': ['Sus scrofa domesticus', 2, "domestic pig"],
	'Procavia capensis': ['Procavia capensis', 3, "dassie"],
	'Propithecus coquereli': ['Propithecus coquereli', 3, "Coquerel`s sifaka"],
	'Procyon sp.': ['Procyon sp.', 2, "raccoon"],
	'Phacochoerus africanus': ['Phacochoerus africanus', 3, "common warthog"],
	
	'Raccoon': ['Procyon sp.', 2, "raccoon"],
	'Red Deer': ['Cervus elaphus', 2, "red deer"],
	'reddeer': ['Cervus elaphus', 2, "red deer"],
	'Red fox': ['Vulpes vulpes', 2, "red fox"],
	'Rhino': [None, 3, "rhinoceros"],
	'rhinoceros': [None, 3, "rhinoceros"],
	'Rudarius ercodes': ['Rudarius ercodes', 3, "whitespotted pygmy filefish"],
	'Rhipicephalus microplus': ['Rhipicephalus microplus', 3, 'Asian blue tick'],
	
	'sea lion': [None, 3, "sea lion"],
	'Sheep': ['Ovis aries', 2, "domestic sheep"],
	'STOAT': ['Mustela erminea', 2, "stoat"],
	'Suricat': ['Suricata suricatta', 2, "meerkat"],
	'Sciurus vulgaris': ['Sciurus vulgaris', 3, "red squirrel"],
	'Sus scrofa domesticus': ['Sus scrofa domesticus', 3, "domestic pig"],
	'Syncerus caffer': ['Syncerus caffer', 3, "African buffalo"],
	'Sparus aurata': ['Sparus aurata', 3, "gilt-head bream"],
	'Sporobolus alterniflorus': ['Sporobolus alterniflorus', 3, "smooth cordgrass"],
	
	'Tragelaphus strepsiceros': ['Tragelaphus strepsiceros', 3, "greater kudu"],

	'Ursus thibetanus': ['Ursus thibetanus', 3, "Asian black bear"],
	
	'Varecia variegata subcincta': ['Varecia variegata subcincta', 3, "white-belted black-and-white ruffed lemur"],
	'Vulpes vulpes': ['Vulpes vulpes', 3, "red fox"],
	
	'waterbuck': ['Kobus ellipsiprymnus', 3, "waterbuck"],
	'wild boar': ['Sus scrofa', 2, "wild boar"],

	'zebrafish': ['Danio rerio', 2, "zebrafish"],

	# DO THESE LAST
	'Sus scrofa': ['Sus scrofa', 2, "wild boar or pig"], # see "Sus scrofa domesticus"
	'Boar': ['Sus scrofa', 2, "wild boar"], # see "wild boar"
	'primate': [None, 2, "non-human primate"], # see NHP
	'POSSUM': [None, 2, 'possum'], # not the same as opossum
	'veterinary': [None, 3, 'unspecified veterinary sample'],
	'Body organs': [None, None, None], # many of these seem to be PRJNA643892 which is 'multispecies'
	'Culture': [None, None, None],
	'Tissue: Lymph nodes': [None, None, None],
	'Tuberculosis': [None, None, None],
	'Laboratory': [None, 2, 'lab strain'],
	'laboratory': [None, 2, 'lab strain'],
	'Lab strain': [None, 2, 'lab strain'],
}