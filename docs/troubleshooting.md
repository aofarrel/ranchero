# Troubleshooting

#### ModuleNotFoundError referencing pyarrow
Upgrade pandas to 2.0.0 or higher *and* make sure you have pyarrow installed

#### Can't read an input file
Certain metadata providers do not double-quote country fields. For this reason, the apostrophe in [Côte d'Ivoire](https://en.wikipedia.org/wiki/C%C3%B4te_d%27Ivoire) can sometimes cause issues. It is recommended to manually grep `Côte d'Ivoire`, `Cote d'Ivoire`, and other iterations of this country into `IVORY COAST`. 

#### Côte d'Ivoire (Ivory Coast) not converting
This may be a parsing issue with the apostrophe in the country's name. Assuming parsing was successful, standardize_coutries() recognizes all of the following as Côte d'Ivoire and will convert them to CIV:
* Côte d'Ivoire
* Cote d'Ivoire
* Cote d_Ivoire
* IVORY COAST
* Ivory Coast
* IVORY_COAST
* Republic of Côte d'Ivoire