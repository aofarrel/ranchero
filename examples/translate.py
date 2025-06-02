import src as Ranchero

RUY5Kinnex = Ranchero.from_tsv('../HPRC_metadata/submissions/RU_Y5_Kinnex/RU_Y5_Kinnex_metadata.tsv', check_index=False, auto_rancheroize=False)
RUY5Kinnex = Ranchero.NeighLib.translate_HPRC_IDs(RUY5Kinnex, 'sample_ID', 'BioSample')
RUY5Kinnex = Ranchero.to_tsv(RUY5Kinnex, '../HPRC_metadata/submissions/RU_Y5_Kinnex/RU_Y5_Kinnex_metadata.tsv')