# Troubleshooting

#### ModuleNotFoundError referencing pyarrow
Upgrade pandas to 2.0.0 or higher *and* make sure you have pyarrow installed

#### Can't read an input file
* Make sure you are using the correct function for the correct input file
* If your file is a TSV or CSV, make sure the number of columns is consistent across all rows
* standardize_countries() will convert any location containing the substring "Ivory" or "Ivoire" into CIV, but sometimes the apostrophe in [Côte d'Ivoire](https://en.wikipedia.org/wiki/C%C3%B4te_d%27Ivoire) will not be delimited properly, resulting in the file itself failing to parse. Consider manually grepping `Côte d'Ivoire`, `Cote d'Ivoire`, and other iterations of this country into `IVORY COAST`. 