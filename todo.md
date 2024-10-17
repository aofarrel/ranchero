RECENT CHANGES
* configuration is actually used now
* single-element lists are un-listed when possible
* removed explosion and made a summary chart after flattening lists as much as possible

HIGH PRIORITY
* check date information isn't garbage


MEDIUM PRIORITY
* merge nextstrain/coscolla/whatever lineage into literature_lineage, mayhaps?
* no longer think ERR181314/SAMEA1573039 is disappearing; should remove prints
* run_file_run or samples using it might be disappearing. it's seemingly got no values during hella flat. not a problem in and of itself since we don't care about that metadata but may indicate issues. but maybe it's null to begin with? then why is it a column?!



LOW PRIORITY
* primary_search seems to be turning into a stringified multi-element list. acceptable as it functions as a partial-match search column but not ideal.
* make isolation source column clean -- like primary_search it's giving stringified multi-element list. might drop column?