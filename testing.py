import polars as pl
data = {"organism": [
	"Mycobacterium", 
	"Mycobacterium [tuberculosis] TKK-01-0051",
	"Mycobacterium avium", 
	"Mycobacterium avium complex sp.",
	"Mycobacterium canettii",
	"Mycobacterium canettii CIPT 140010059",
	"Mycobacterium smegmatis",
	"Mycobacterium sp. DSM 3803",
	"Mycobacterium tuberculosis",
	"Mycobacterium tuberculosis 102",
	"Mycobacterium tuberculosis 210_4C15_16C1_56C2",
	"Mycobacterium tuberculosis str. Erdman = ATCC 35801",
	"Mycobacterium tuberculosis variant africanum",
	"Mycobacterium tuberculosis variant bovis",
	"Mycobacterium tuberculosis variant bovis BCG",
	"Mycobacterium tuberculosis variant pinnipedii"
],
"desired": [
	"Mycobacterium",
	"Mycobacterium avium complex",
	"Mycobacterium avium", 
	"Mycobacterium avium complex sp.",
	"Mycobacterium canettii",
	"Mycobacterium canettii",
	"Mycobacterium smegmatis",
	"Mycobacterium sp. DSM 3803",
	"Mycobacterium tuberculosis",
	"Mycobacterium tuberculosis",
	"Mycobacterium tuberculosis",
	"Mycobacterium tuberculosis",
	"Mycobacterium tuberculosis variant africanum",
	"Mycobacterium tuberculosis variant bovis",
	"Mycobacterium tuberculosis variant bovis",
	"Mycobacterium tuberculosis variant pinnipedii"
]
}
df = pl.DataFrame(data)
df = Ranchero.rm_tuberculosis_suffixes(df)
df = df.with_columns(pl.when(pl.col("organism") == pl.col("desired")).then(pl.lit("-")).otherwise(pl.lit("FAIL")).alias("correct"))
Ranchero.super_print(df, "organism cleanup")

