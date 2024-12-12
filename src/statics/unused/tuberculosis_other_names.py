# These are given their own seperate page because they are either ambigious terms which
# could flag a lot of false positives, or use non-ASCII character encodings. Regardless,
# this should not be considered a comprehensive, exhaustive, nor authoritative resource.

specific = [
	"结核病", # Simplified Chinese
	"結核", # Japanese
	"결핵", # Korean
	"Туберкулёз", # Russian
	"यक्ष्मा", # Hindi
	"காச நோய்", # Tamil
	"যক্ষ্মা" # Bengali
]

# These words may have more than one meaning
potentially_ambigious = [
	"consumption", # archaic term
	"Lao", # Vietnamese
	"phthisis", # archaic medical term
	"سل", # Arabic
	"ٹی بی" # Urdu
]