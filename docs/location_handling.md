# Locations (countries, states, regions, continents, cities, etc)
Standardizing location-based metadata has several headaches:
* Some uploaders use one field for country, and another for a more specific region, such as "South Korea" in a country field and "Seoul" for a city field
* Some uploaders use the same field for all location based metadata, such as "South Korea: Seoul"
* Some countries have states/prefectures/administrative regions, others do not
	* And even samples that are from the same country will vary as to whether or not they include some of these levels -- a sample from the city of San Francisco might be labeled "San Francisco", "USA - San Francisco", "USA, California, San Francisco", etc
* Some samples are from disputed regions, or from regions that were previously identified with one country but are now considered part of another
* Not all countries are universally recognized
* Countries sometimes change name
* Some location names use special characters, which uploaders may or may not include
* Some countries are substrings of other countries, such as "Republic of Congo" (COG) versus "Democratic Republic of Congo" (COD)
* Some countries (such as Russia) are often considered part of multiple continents
* Sometimes, NCBI uploaders will interpret commonly used NCBI fields referencing "location" to mean the location of the body a sample was isolated, rather than geographic location

In attempt to handle the complexity of these situations, Ranchero first uses a list of common NCBI column names to gather all of the most common columns used for geographic location, as classified in [/src/ranchero/statics/kolumns.py](/src/ranchero/statics/kolumns.py), then does string matching along these principles:
1. Take the submitter at their word, unless it's too ambiguous or a clear mistake
	* That is to say: If a submitter says a sample is from location X, we treat it as if it is from location X, regardless of current borders
2. If we find a match to a geopolitical entity with a widely-used ISO 3166 country code, we consider that to be the value of "country," and convert it to that ISO 3166 code
	* We convert a handful of commonly used non-ISO 3166 acronyms
	* If a country's name has changed sometime after the late-20th century, match upon the old and new name
3. Continents are assigned according to NCBI standards, which means every country is assigned to precisely one continent
	* For example, NCBI considers Russia to be in the continent of Europe and Panama to be in the continent of North America
4. We use "region" as a generic term to mean anything smaller than a country, including state, county, city, village, etc
5. Any location-based data that fails to match a country or continent, that isn't otherwise eliminated for ambiguity, is considered a region
6. Ambiguity is handled on a case-by-case basis (see below for specific examples)
7. Columns that are usually associated with tissue type but occasionally have geographic information, such as isolation_source, are checked for geographic information but require stricter matches

For more information about precisely how matching is performed, such as the order of events or where substring-versus-exact matches are used, refer to standardize_countries() in the source code.

## Examples and Exceptions

### "Take the submitter at their word" exceptions
A small handful of common typos (or variations arising from language differences) are converted to their most likely intended value. For example, "Ethopia" is considered a typo for "Ethiopia".

#### Special handling for [Côte d'Ivoire (Ivory Coast)](https://en.wikipedia.org/wiki/Ivory_Coast)
There is an apostrophe, a space, and an ô in Côte d'Ivoire (CIV), which a lot of software struggle to handle -- including the software used by submitters. As such, there are many variations in this particular country's name, such as `Cote D Ivoire`, `Cote d\'Ivoire` (with literal \ included in the plaintext file), and `IVORY_COAST`. In attempt to cover all possible variations, and with the knowledge "Ivory" is not common in location names, Ranchero will convert anything matching the substring `Ivory` or `Ivoire` to CIV.

#### Special handling for countries with substring matches
Substring matches are surprisingly important for NCBI data, which quite frequently is in "Country: Region" or "Country, Region" or "Region (Country)" format, and due to the limitations of polars' interpretation of regular expressions. However, extra care is needed for a handful of countries with problematic substring matches, for example:
* `Guinea` --> wholestring match is assumed to be Guinea (GIN), avoid substring matches to Papua New Guinea (PNG), Guinea-Bissau (GNB), and Equatorial Guinea (GNQ)
* `Mali` --> avoids substring matches to avoid matches with So**mali**a and So**mali**land
* `Mexico` --> avoids substring matches to avoid matches with the US state New Mexico

#### Special handling for Democratic Republic of the Congo/Republic of Congo
The [Democratic Republic of the Congo](https://en.wikipedia.org/wiki/Democratic_Republic_of_the_Congo) (henceforth COD) and the [Republic of Congo](https://en.wikipedia.org/wiki/Republic_of_the_Congo) (henceforth COG) are frequently confused. Further complicating matters is the fact that COG's full name is a substring of COD's full name, and "the Congo" is sometimes informally used to mean either. As such we need special rules for this region.
* We assume submitters who write "Republic of Congo" do indeed mean COG, even though it is possible they actually meant "Democratic Republic of Congo" (COD)
	* Justification: If we do not do this, essentially no COG samples will be identified. Furthermore, this follows the principle of "take the submitter at their word."
* We consider "Republic of Congo" and "Republic of the Congo" (without "Democratic" in front) to both be COG
* We consider "DRC" to be Democratic Republic of the Congo (COD)
* We consider "Congo" and "the Congo" on its own to be too ambiguous and assign them no country code, even though "Congo" is often used for COG and "the Congo" is often used for COD, as these names are too frequently flipped to assign with confidence

