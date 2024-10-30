RECENT CHANGES
* nullify now acts on strings within lists!

HIGH PRIORITY
* there is a column called "literal" sneaking in, probably from a pl.lit()
* check date information isn't garbage
* denylist SAMN33804027 and friends 

MEDIUM PRIORITY
* parse host_overrides.tsv
* add a row counter to make show rows aren't vanishing (merge function kinda checks this though)
* merge nextstrain/coscolla/whatever lineage into literature_lineage, mayhaps?
* run_file_run or samples using it might be disappearing. it's seemingly got no values during hella flat. not a problem in and of itself since we don't care about that metadata but may indicate issues. but maybe it's null to begin with? then why is it a column?!



LOW PRIORITY
* primary_search seems to be turning into a stringified multi-element list. acceptable as it functions as a partial-match search column but not ideal.
* make isolation source column clean -- like primary_search it's giving stringified multi-element list. might drop column?




