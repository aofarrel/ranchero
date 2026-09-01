import polars as pl
from src import ranchero as Ranchero
# this assumes empty lists, empty strings, and total garbage ("uncalculated" etc) already dropped
# maybe retain input raw columns should be done upstream?

def tempcol(polars_df, name, error=True):
        """
        Return a string of a valid temporary column name, trying user-specified string first.
        If error, raise an error if user-specificed string isn't available.
        """
        candidates = [name, "temp", "foo", "bar", "tmp1", "tmp2", "scratch"]
        for candidate in candidates:
            if candidate not in polars_df.columns:
                return candidate
            elif candidate == name and error:
                raise ValueError(f"Could not generate temporary column called {name} as that name is already taken")
        raise ValueError("Could not generate a temporary column")


# str.replace_many(): match on substrings, case insensitive, cannot overwrite with null, if no match returns match column
# * Good for: ISO standardization of country
# * Bad for: Basically everything else
#┌───────────────────────────────┬─────────────────────────────────┬───────────────────────────┐
#│ test_case                     ┆ region                          ┆ isolation_source          │
#│ ---                           ┆ ---                             ┆ ---                       │
#│ str                           ┆ str                             ┆ str                       │
#╞═══════════════════════════════╪═════════════════════════════════╪═══════════════════════════╡
#│ wayward substring wrongcase   ┆ Blood from patient              ┆ null                      │
#│ wayward substring rightcase   ┆ blood draw                      ┆ blood draw from a patient │
#│ wayward substring incorrect   ┆ The National Heart and Blood C… ┆ null                      │
#│ wayward exact rightcase       ┆ blood                           ┆ null                      │
#│ wayward exact wrongcase       ┆ Blood                           ┆ null                      │
#│ real country                  ┆ Cote d'Ivoire                   ┆ null                      │
#│ wayward exact needs overwrite ┆ Affedcted Herd                  ┆ left shoulder             │
#│ none country                  ┆ null                            ┆ null                      │
#└───────────────────────────────┴─────────────────────────────────┴───────────────────────────┘
# wayward = {"foo": "bar", "chicken": "dance", "blood": "blood", "Affedcted Herd": "vetrinary"}
# pl.col(match_col).str.replace_many(wayward_keys, dictionary_values, ascii_case_insensitive=True).alias(temp_correct_col)
#
#┌───────────────────────────────┬─────────────────────────────────┬───────────────────────────┬─────────────────────────────────┐
#│ test_case                     ┆ region                          ┆ isolation_source          ┆ tmp_matches                     │
#│ ---                           ┆ ---                             ┆ ---                       ┆ ---                             │
#│ str                           ┆ str                             ┆ str                       ┆ str                             │
#╞═══════════════════════════════╪═════════════════════════════════╪═══════════════════════════╪═════════════════════════════════╡
#│ wayward substring wrongcase   ┆ Blood from patient              ┆ null                      ┆ blood from patient              │
#│ wayward substring rightcase   ┆ blood draw                      ┆ blood draw from a patient ┆ blood draw                      │
#│ wayward substring incorrect   ┆ The National Heart and Blood C… ┆ null                      ┆ The National Heart and blood C… │
#│ wayward exact rightcase       ┆ blood                           ┆ null                      ┆ blood                           │
#│ wayward exact wrongcase       ┆ Blood                           ┆ null                      ┆ blood                           │
#│ real country                  ┆ Cote d'Ivoire                   ┆ null                      ┆ Cote d'Ivoire                   │
#│ wayward exact needs overwrite ┆ Affedcted Herd                  ┆ left shoulder             ┆ vetrinary                       │
#│ none country                  ┆ null                            ┆ null                      ┆ null                            │
#└───────────────────────────────┴─────────────────────────────────┴───────────────────────────┴─────────────────────────────────┘


def setup_polars_expressions(polars_df, match_col, correct_col, dictionary, overwrite):
    dictionary_keys = list(dictionary.keys())
    dictionary_values = list(dictionary.values())
    temp_correct_col = tempcol(polars_df, "tmp_matches")

    # str.replace_many() doesn't allow us to replace matches with null, so removing matches requires we write to a temporary column

    if overwrite:
        if polars_df.schema[correct_col] == pl.Utf8:
            move_to_correct_col = pl.col(match_col).str.replace_many(dictionary_keys, dictionary_values, ascii_case_insensitive=True).alias(temp_correct_col)
        elif polars_df.schema[correct_col] == pl.List(pl.Utf8):
            move_to_correct_col = pl.col(match_col).list.str.replace_many(dictionary_keys, dictionary_values, ascii_case_insensitive=True).alias(temp_correct_col)
        else:
            raise TypeError
    else:
        if polars_df.schema[correct_col] == pl.Utf8:
            move_to_correct_col = pl.when(pl.col(correct_col).is_null()).then(pl.col(match_col).str.replace_many(dictionary_keys, dictionary_values, ascii_case_insensitive=True)).otherwise(pl.col(correct_col)).alias(correct_col)
        elif polars_df.schema[correct_col] == pl.List(pl.Utf8):
            move_to_correct_col = pl.when(pl.col(correct_col).is_null()).then(pl.col(match_col).str.replace_many(dictionary_keys, dictionary_values, ascii_case_insensitive=True)).otherwise(pl.col(correct_col)).alias(correct_col)
        else:
            raise TypeError

    if polars_df.schema[match_col] == pl.Utf8:
            drop_from_match_col = pl.col(match_col).str.replace_many(dictionary_keys, [''], ascii_case_insensitive=True).alias(match_col)
    elif polars_df.schema[match_col] == pl.List(pl.Utf8):
        drop_from_match_col = pl.col(match_col).str.replace_many(dictionary_keys, [''], ascii_case_insensitive=True).alias(match_col)
    else:
        raise TypeError

    return drop_from_match_col, move_to_correct_col



polars_df = pl.DataFrame(
    {
        "match_type": [
            "substring",
            "substring, iso not null",
            "substring",
            "exact",
            "exact",
            "exact region",
            "substring region",
            "exact match, iso not null",
            "None"
        ],

        "region": [
            "Blood from patient",
            "blood draw",
            "The National Heart and Blood Clinic",
            "blood",
            "Blood",
            "Cote d'Ivoire",
            "Cote d'Ivoire Hospital",
            "Affedcted Herd",
            None
        ],

#        "region_list": [
#            ["Blood from patient"],
#            ["blood draw"],
#            ["The National Heart and Blood Clinic"],
#            ["blood"],
#            ["Blood"],
#            ["Cote d'Ivoire"],
#            ["Affedcted Herd"],
#            None
#        ],

        "isolation_source": [
            None,
            "blood draw from a patient",
            None,
            None,
            None,
            None,
            None,
            "left shoulder",
            None
        ],
    }
)

polars_df = pl.DataFrame({
    "situation": ["substring", "substring; write_col not null", "exact", "exact; write_col not null", "should NOT be replaced", "None"],
    "region": ["Blood from patient", "blood draw", "blood", "Affedcted Herd", "Cote d'Ivoire", None],
    "isolation_source": [None, "blood draw from a patient", None, "left shoulder", None, None]})
isolation_source_replacements = {"blood": "blood", "Affedcted Herd": "vetrinary"}
country_replacements = {"Cote d'Ivoire": 'CIV'}

print("Input dataframe")
print(polars_df)

match_col = "region"
write_col = "region_result"
print("Region, isolation_source dictionary, substrings true, overwrite false")
print(Ranchero.Standardizer.dictionary_match(polars_df, match_col, write_col, isolation_source_replacements, substrings=True, overwrite=False))

print("Region, isolation_source dictionary, substrings false, overwrite false")
print(Ranchero.Standardizer.dictionary_match(polars_df, match_col, write_col, isolation_source_replacements, substrings=False, overwrite=False))

print("Region, isolation_source dictionary, substrings true, overwrite true")
print(Ranchero.Standardizer.dictionary_match(polars_df, match_col, write_col, isolation_source_replacements, substrings=True, overwrite=True))

print("Region, isolation_source dictionary, substrings false, overwrite true")
print(Ranchero.Standardizer.dictionary_match(polars_df, match_col, write_col, isolation_source_replacements, substrings=False, overwrite=True))

"""
match_col, write_col = "region", "isolation_source"
has_a_match = pl.col(match_col).str.contains_any(list(good_replacements.keys()), ascii_case_insensitive=True)
str_replace_many = pl.col(match_col).str.replace_many(list(good_replacements.keys()), list(good_replacements.values()), ascii_case_insensitive=True)

print("Not great at simplifying good stuff")
print(polars_df.with_columns(pl.when( (pl.col(write_col).is_null()).and_(has_a_match) ).then(str_replace_many).otherwise(write_col).alias(write_col)))


match_col, write_col = "region", "country"
has_a_match = pl.col(match_col).str.contains_any(list(bad_replacements.keys()), ascii_case_insensitive=True)
str_replace_many = pl.col(match_col).str.replace_many(list(bad_replacements.keys()), list(bad_replacements.values()), ascii_case_insensitive=True)
print("Might be good at bringing bad stuff into the right column")
polars_df = polars_df.with_columns(pl.lit(None).alias("country"))
print(polars_df.with_columns(pl.when( (pl.col(write_col).is_null()).and_(has_a_match) ).then(str_replace_many).otherwise(write_col).alias(write_col)))
"""
#polars_df = Ranchero.Standardizer.dictionary_match(polars_df, match_col, write_col, wayward, substrings=True, overwrite=False)
#print(polars_df)

"""
drop_from_match_col, move_to_correct_col = setup_polars_expressions(polars_df, "region", "isolation_source", wayward, True)
polars_df = polars_df.with_columns(move_to_correct_col)
print(polars_df)
polars_df = polars_df.with_columns(drop_from_match_col)
print(polars_df)
"""



