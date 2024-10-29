########## drop zone ##############
# columns in here will be dropped
tb_but_not_used = ['mycobacteriaceae_family_sam', 'mycobacterium_genus_sam']
datastore = ['datastore_filetype', 'datastore_region']

mildly_problematic = ['biosamplemodel', 'run_file_version', 'host_disease_sam', 'library_name']

sample_ids_that_are_not_uuids = [
	'run_id_run',
	'filename2_sam',
	'uniqueidentifier_sam',
	'gold_stamp_id_sam',
	'seek_uid_sam',
	'individual_sam',
	'complete_sample_name_sam',
	'raw_sample_name_sam',
	'accession_number_sam',
	'alias_sam',
	'alt_id_sam',
	'alternate_id_sam',
	'anonymised_bovine_id_sam',
	'anonymized_name_sam',
	'assemblyname',
	'bi_gssr_sample_ids_exp',
	'bi_gssr_sample_id_sam',
	'bi_gssr_sample_lsid_sam',
	'bi_work_request_id_exp',
	'biological_replicate_sam',
	'biosample_sam',
	'borstel_sample_id_sam',
	'cell_index_run',
	'cultures_sam',
	'dna_seek_uid_sam',
	'ena_checklist_sam_s_dpl67',
	'isolate_name_alias_sam',
	'extra_sam',
	'fasta_file_run',
	'filename_sam',
	'genome_number_sam',
	'gssr_id_run',
	'id_sam',
	'internal_project_id_sam',
	'isolate__name_or_label_sam',
	'lab_id_sam',
	'label_sam',
	'lsid_exp',
	'lsid_run',
	'lsid_sam',
	'lsids_exp',
	'md5_checksum2_run',
	'md5_checksum_run',
	'original_id_sam',
	'plate_id_run',
	'pmid_sam',
	'project_exp',
	'projects_exp',
	'projects_run',
	'rea_sam',
	'read_group_id_run',
	'read_group_platform_unit_run',
	'region_run',
	'research_project_exp',
	'research_project_run',
	'root_sample_id_exp',
	'root_sample_id_run',
	'run_barcode_run',
	'run_name_run',
	'run_run',
	'sample_alias_sam_s_dpl38',
	'sample_id_exp',
	'sample_id_number_sam',
	'sample_id_run',
	'sample_id_sam',
	'sample_name',
	'submitted_subject_id_sam',
	'datasetid_sam',
	'sample_name_sam',
	'sample_number_sam_s_dpl45',
	'sample_sam',
	'sample_title_sam',
	'sampleid_sam',
	'source_material_id_sam_s_dpl48',
	'specimen_voucher_sam',
	'sra_accession_sam',
	'sra_title_run',
	'study_id_sam_s_dpl50',
	'subject_id_sam_s_dpl1092',
	'tb_portal_id_sam',
	'title_sam',
	'culture_id_sam',
	'tubeid_sam',
	'library_id_sam',
	'extraction_sam',
	'well_run',
	'work_request_exp',
	'work_request_id_run',
	'work_request_run',
	'uniqid_sam',
	'uniqueid_sam',
]

redundant = [
	'accession_run',
	'analysis_type_exp',
	'gssr_id_exp',
	'instrument_model_run',
	'library_type_exp',
	'material_type_exp',
	'project_id_run',
	'project_name_sam',
	'project_run',
	'rank',  # taxid rank
	'sample_name_run',
	'sample_types_exp',
]

submitter = [
	'biomaterial_provider_sam',
	'broker_name_sam',
	'center_name_exp',
	'center_name_insdc',
	'center_project_name_exp',
	'colaborators_sam',
	'collaborator_id_sam',
	'collected_by',
	'collected_by_sam',
	'collected_by_sam',
	'collected_by_sam_s_dpl136',
	'identified_by_sam',
	'initiative_run',
	'insdc_center_alias_sam',
	'insdc_status_sam',
	'lab_host_sam_s_dpl270',
	'sequencing_funding_program_name_run',
	'submitter_id_sam',
	'submitter_id_sam',
	'supplier_name_sam',
	'kamruddin_ahmed_sam',
	'jaeyres_jani_sam',
	'chin_kai_ling_sam',
	'naing_oo_tha_sam',
	'ong_twee_hee_rick_sam',
	'richard_avoi_sam',
	'valentine_gantul_sam',
	'zainal_arifin_mustapha_sam'
]


cell_growth_stuff = [
	'antibiotic_sam',
	'antibody_sam_ss_dpl59',
	'cell_type_sam_ss_dpl37',
	'concentration_sam',
	'culture_conditions_sam',
	'culture_id_sam',
	'culture_media_sam',
	'samp_vol_we_dna_ext_sam',
	'dose_sam',
	'drug_sam',
	'drug_treatment_sam_s_dpl85',
	'evaluation_sam',
	'exposure_time_sam',
	'extraction_sam',
	'forward_seq_sam',
	'growth_condition_sam',
	'growth_media_sam',
	'culture_conditions_sam',
	'experimental_factor__dose_exp',
	'growth_media_sam',
	'growth_phase_sam_s_dpl215',
	'growth_protocol_sam_s_dpl216',
	'batch_sam_s_dpl12',
	'host_subject_id_sam',
	'individual_sam',
	'isol_growth_condt_sam',
	'culture_collection_sam_ss_dpl150',
	'library_id_sam',
	'num_replicons_sam',
	'culture_media_sam',
	'passage_history_sam_s_dpl312',
	'ref_biomaterial_sam',
	'isol_growth_condt_sam',
	'replicate_sam_ss_dpl121',
	'reverse_seq_sam',
	'samp_size_sam',
	'temp_sam',
	'time_sam',
	'treatment_sam_ss_dpl55'
]

metadata_dates = [ # NOT SAMPLE COLLECTION DATES!
	'ena_first_public_run',
	'ena_first_public_sam',
	'ena_first_public_sam',
	'ena_first_public_sam_dt_dpl178',
	'ena_first_public_sam_dt_dpl178',
	'ena_last_update_run',
	'ena_last_update_sam',
	'ena_last_update_sam',
	'ena_last_update_sam_ss_dpl537',
	'ena_last_update_sam_ss_dpl537',
	'insdc_first_public_sam',
	'insdc_last_update_sam',
]

other = [
	'napier_type', # causes issues with the Napier dataset
	'actual_read_length_run',
	'patient_inclusion_sam',
	'transcription_factor_expression_level_sam',
	'sex_calc',
	'eight_pac_barcode_run',
	'pacbio_rs_sequencing_kit_barcode_run',
	'host_tot_mass_sam',
	'additional_information_sam',
	'pacbio_rs_binding_kit_exp',
	'pacbio_rs_template_prep_kit_barcode_exp',
	'pacbio_rs_template_prep_kit_exp',
	'pacbio_rs_binding_kit_barcode_exp',
	'template_preparation_kit_insert_size_exp',
	'afb_score_sam',
	'alignment_software_exp',
	'aliquot_sam_s_dpl57',
	'analysis_type_run',
	'antibody_manufacturer_sam',
	'antibody_sam_ss_dpl59',
	'antibiotic_treatment_sam',
	'pipe_material_sam',
	'mouse_pool_sam',
	'breed_sam',
	'exposure_time_sam',
	'concentration_sam',
	'dose_sam',
	'antibiotic_sam',
	'quality_book_char_run',
	'arrayexpress_sex_sam_s_dpl7',
	'arrayexpress_organismpart_sam',
	'gender_and_age_sam',
	'dissolved_oxygen_sam',
	'strain_background_sam',
	'treatment_sam_ss_dpl55',
	'environmental_sample_sam',
	'environment__biome__sam',
	'biome_sam',
	'extraction_sam',
	'evaluation_sam',
	'developmental_stage_sam',
	'plaque_index_run',
	'sewage_type_sam',
	'patient_number_sam_s_dpl111',
	'name_of_the_sampling_site_sam',
	'arrayexpress_strainorline_sam',
	'attribute__time_sam',
	'bindingkit_exp',
	'patient_finished_treatment_sam',
	'patient_has_hiv_sam',
	'time_sam',
	'patientid_sam',
	'camp_treatment_level_sam',
	'cfz_mic_sam',
	'samp_size_sam',
	'passage_history_sam_s_dpl312',
	'reverse_seq_sam',
	'forward_seq_sam',
	'host_sex_sam',
	'sequencing_sample_type_sam',
	'ref_biomaterial_sam',
	'num_replicons_sam',
	'chip_antibody_sam_ss_dpl428',
	'rel_to_oxygen_sam',
	'perturbation_sam',
	'reverse_reads_sam',
	'forward_read_sam',
	'collection_number_run',
	'collection_protocol_run',
	'comment_sam',
	'concentration__um__sam',
	'day_sam_s_dpl14',
	'depth_sam',
	'description_sam',
	'dna_concentrations_sam',
	'estimated_size_sam',
	'host_spec_range_sam',
	'flowcell_barcode_run',
	'forward_read_length_run',
	'identification_method_sam',
	'instctrlver_exp',
	'instrument_name_run',
	'lane_run',
	'locus_tag_prefix_sam',
	'metadata_attribute_sam',
	'molecular_indexing_scheme_run',
	'molecular_indexing_schemes_run',
	'molecule_subtype_sam',
	'number_sam',
	'passage_sam',
	'patient_id_sam_s_dpl110',
	'position_in_the_box_sam',
	'product_order_exp',
	'product_order_run',
	'product_part_number_exp',
	'product_part_number_run',
	'protein_concentration_sam',
	'protein_sam',
	'reverse_read_length_run',
	'samp_dis_stage_sam',
	'samp_mat_process_sam',
	'sample_description_sam',
	'sampling_sam_s_dpl133',
	'samplingday_sam',
	'sequencing_sam',
	'sequencingkit_exp',
	'silt_____sam',
	'sand_____sam',
	's__mg_dm__3___sam',
	'ph__cacl_2___sam',
	'nematicides_sam',
	'mn__mg_dm__3___sam',
	'mg__mmol_c__dm__3___sam',
	'k__mmol_c__dm__3___sam',
	'insecticides_sam',
	'fungicides_sam',
	'fine_sand_____sam',
	'fertilizers_sam',
	'fe__mg_dm__3___sam',
	'environment__material__sam',
	'investigation_type_sam',
	'material_sam',
	'cu__mg_dm__3___sam',
	'cropping_system_sam',
	'cec__mmol_c__dm__3___sam',
	'ca__mmol_c__dm__3___sam',
	'base_saturation__v____sam',
	'b__mg_dm__3___sam',
	'agrochemicals_sam',
	'mgap_id_sam',
	'transfection_construct_sam',
	'time_post_wetup_sam',
	'clay_____sam',
	'coarse_sand_____sam',
	'synthetical_inputs_sam',
	'tf_induction_state_sam',
	'tn_library_racks_sam',
	'tn_mutant_library_sam',
	'zmw_set_run',
]

silly_columns = other + redundant + mildly_problematic + tb_but_not_used + datastore + metadata_dates + sample_ids_that_are_not_uuids + submitter + cell_growth_stuff

clearly_not_tuberculosis = [
	'abiotrophia_defectiva_species_sam',
	'abiotrophia_genus_sam',
	'acetobacteraceae_family_sam',
	'acetobacteraceae_unclassified_genus_sam',
	'achromobacter_genus_sam',
	'achromobacter_xylosoxidans_species_sam',
	'acidaminococcaceae_family_sam',
	'acidaminococcaceae_unclassified_genus_sam',
	'acidaminococcus_fermentans_species_sam',
	'acidaminococcus_genus_sam',
	'acidaminococcus_intestini_species_sam',
	'acidaminococcus_sp_d21_species_sam',
	'acidaminococcus_unclassified_species_sam',
	'acidobacteriaceae_family_sam',
	'acidovorax_ebreus_species_sam',
	'acidovorax_genus_sam',
	'acinetobacter_bereziniae_species_sam',
	'acinetobacter_bouvetii_species_sam',
	'acinetobacter_brisouii_species_sam',
	'acinetobacter_genus_sam',
	'acinetobacter_guillouiae_species_sam',
	'acinetobacter_indicus_species_sam',
	'acinetobacter_johnsonii_species_sam',
	'acinetobacter_unclassified_species_sam',
	'actinobaculum_massiliense_species_sam',
	'actinobaculum_schaalii_species_sam',
	'actinobaculum_sp_oral_taxon_183_species_sam',
	'actinobaculum_unclassified_species_sam',
	'actinobaculum_urinale_species_sam',
	'adlercreutzia_equolifaciens_species_sam',
	'aerococcaceae_family_sam',
	'aerococcus_urinae_species_sam',
	'aeromonadaceae_family_sam',
	'aeromonas_genus_sam',
	'aeromonas_hydrophila_species_sam',
	'aeromonas_unclassified_species_sam',
	'afipia_broomeae_species_sam',
	'afipia_genus_sam',
	'afipia_unclassified_species_sam',
	'aggregatibacter_genus_sam',
	'akkermansia_genus_sam',
	'akkermansia_muciniphila_species_sam',
	'alcaligenaceae_family_sam',
	'alcaligenes_unclassified_species_sam',
	'alicycliphilus_genus_sam',
	'alicycliphilus_unclassified_species_sam',
	'alistipes_finegoldii_species_sam',
	'alistipes_genus_sam',
	'alistipes_indistinctus_species_sam',
	'alistipes_onderdonkii_species_sam',
	'alistipes_putredinis_species_sam',
	'alistipes_senegalensis_species_sam',
	'alistipes_shahii_species_sam',
	'alistipes_sp_ap11_species_sam',
	'alistipes_sp_hgb5_species_sam',
	'alistipes_unclassified_species_sam',
	'alkaliphilus_genus_sam',
	'alloprevotella_tannerae_species_sam',
	'alloprevotella_unclassified_species_sam',
	'alloscardovia_omnicolens_species_sam',
	'alphapapillomavirus_3_species_sam',
	'anaerococcus_genus_sam',
	'anaerococcus_hydrogenalis_species_sam',
	'anaerococcus_lactolyticus_species_sam',
	'anaerococcus_obesiensis_species_sam',
	'anaerococcus_prevotii_species_sam',
	'anaerococcus_tetradius_species_sam',
	'anaerococcus_vaginalis_species_sam',
	'anaerofustis_genus_sam',
	'anaerofustis_stercorihominis_species_sam',
	'anaeroglobus_geminatus_species_sam',
	'anaerostipes_genus_sam',
	'anaerostipes_hadrus_species_sam',
	'anaerostipes_sp_3_2_56faa_species_sam',
	'anaerostipes_unclassified_species_sam',
	'anaerotruncus_colihominis_species_sam',
	'anaerotruncus_genus_sam',
	'anaerotruncus_unclassified_species_sam',
	'anyantimicrobials_15to30days_rf_sam',
	'anyantimicrobials_1to3days_rf_sam',
	'anyantimicrobials_4to14days_rf_sam',
	'anyantimicrobials_sam',
	'arcobacter_butzleri_species_sam',
	'arcobacter_genus_sam',
	'arcobacter_unclassified_species_sam',
	'atopobium_genus_sam',
	'atopobium_minutum_species_sam',
	'atopobium_parvulum_species_sam',
	'atopobium_rimae_species_sam',
	'atopobium_sp_bv3ac4_species_sam',
	'atopobium_vaginae_species_sam',
	'aurantimonadaceae_family_sam',
	'aurantimonadaceae_unclassified_genus_sam',
	'azithro_15to30days_rf_sam',
	'azithro_1to3days_rf_sam',
	'azithro_4to14days_rf_sam',
	'azithro_brf_sam',
	'azithro_sam',
	'bacteroidaceae_family_sam',
	'bacteroidales_bacterium_ph8_species_sam',
	'bacteroides_caccae_species_sam',
	'bacteroides_cellulosilyticus_species_sam',
	'bacteroides_clarus_species_sam',
	'bacteroides_coprocola_species_sam',
	'bacteroides_dorei_species_sam',
	'bacteroides_eggerthii_species_sam',
	'bacteroides_faecis_species_sam',
	'bacteroides_finegoldii_species_sam',
	'bacteroides_fluxus_species_sam',
	'bacteroides_fragilis_species_sam',
	'bacteroides_genus_sam',
	'bacteroides_intestinalis_species_sam',
	'bacteroides_massiliensis_species_sam',
	'bacteroides_nordii_species_sam',
	'bacteroides_oleiciplenus_species_sam',
	'bacteroides_ovatus_species_sam',
	'bacteroides_pectinophilus_species_sam',
	'bacteroides_plebeius_species_sam',
	'bacteroides_salyersiae_species_sam',
	'bacteroides_sp_1_1_30_species_sam',
	'bacteroides_sp_1_1_6_species_sam',
	'bacteroides_sp_3_1_19_species_sam',
	'bacteroides_sp_3_2_5_species_sam',
	'bacteroides_stercoris_species_sam',
	'bacteroides_thetaiotaomicron_species_sam',
	'bacteroides_uniformis_species_sam',
	'bacteroides_vulgatus_species_sam',
	'bacteroides_xylanisolvens_species_sam',
	'barnesiella_intestinihominis_species_sam',
	'beijerinckiaceae_family_sam',
	'beijerinckiaceae_unclassified_genus_sam',
	'betapapillomavirus_1_species_sam',
	'betapapillomavirus_2_species_sam',
	'betapapillomavirus_3_species_sam',
	'betapapillomavirus_4_species_sam',
	'betapapillomavirus_5_species_sam',
	'bifidobacteriaceae_family_sam',
	'bifidobacterium_adolescentis_species_sam',
	'bifidobacterium_angulatum_species_sam',
	'bifidobacterium_animalis_species_sam',
	'bifidobacterium_bifidum_species_sam',
	'bifidobacterium_breve_species_sam',
	'bifidobacterium_catenulatum_species_sam',
	'bifidobacterium_dentium_species_sam',
	'bifidobacterium_genus_sam',
	'bifidobacterium_longum_species_sam',
	'bifidobacterium_minimum_species_sam',
	'bifidobacterium_pseudocatenulatum_species_sam',
	'bifidobacterium_pseudolongum_species_sam',
	'bilophila_genus_sam',
	'bilophila_unclassified_species_sam',
	'bilophila_wadsworthia_species_sam',
	'blautia_genus_sam',
	'blautia_hansenii_species_sam',
	'blautia_hydrogenotrophica_species_sam',
	'blautia_producta_species_sam',
	'bombyx_mori_nucleopolyhedrovirus_species_sam',
	'bordetella_genus_sam',
	'brachyspira_genus_sam',
	'brachyspiraceae_family_sam',
	'bradyrhizobiaceae_family_sam',
	'bradyrhizobium_genus_sam',
	'bradyrhizobium_sp_dfci_1_species_sam',
	'brevibacteriaceae_family_sam',
	'brevibacterium_genus_sam',
	'brevibacterium_massiliense_species_sam',
	'brevibacterium_mcbrellneri_species_sam',
	'brevibacterium_unclassified_species_sam',
	'brucella_genus_sam',
	'brucella_pinnipedialis_species_sam',
	'brucellaceae_family_sam',
	'bulleidia_extructa_species_sam',
	'bulleidia_genus_sam',
	'burkholderia_genus_sam',
	'burkholderia_unclassified_species_sam',
	'burkholderiaceae_family_sam',
	'burkholderiales_bacterium_1_1_47_species_sam',
	'butyricicoccus_pullicaecorum_species_sam',
	'butyrivibrio_crossotus_species_sam',
	'butyrivibrio_genus_sam',
	'c2likevirus_unclassified_species_sam',
	'campylobacter_concisus_species_sam',
	'campylobacter_curvus_species_sam',
	'campylobacter_genus_sam',
	'campylobacter_gracilis_species_sam',
	'campylobacter_hominis_species_sam',
	'campylobacter_showae_species_sam',
	'campylobacter_ureolyticus_species_sam',
	'campylobacteraceae_family_sam',
	'candida_albicans_species_sam',
	'candida_glabrata_species_sam',
	'candida_tropicalis_species_sam',
	'carba_15to30days_rf_sam',
	'carba_1to3days_rf_sam',
	'carba_4to14days_rf_sam',
	'carba_brf_sam',
	'carba_sam',
	'carnobacteriaceae_family_sam',
	'catenibacterium_genus_sam',
	'catenibacterium_mitsuokai_species_sam',
	'cellulophaga_genus_sam',
	'chicken_anemia_virus_species_sam',
	'chlamydiaceae_family_sam',
	'chlamydiaceae_unclassified_genus_sam',
	'chryseobacterium_genus_sam',
	'chryseobacterium_unclassified_species_sam',
	'citrobacter_freundii_species_sam',
	'citrobacter_genus_sam',
	'citrobacter_unclassified_species_sam',
	'clavispora_lusitaniae_species_sam',
	'clostridiaceae_bacterium_jc118_species_sam',
	'clostridiaceae_family_sam',
	'clostridiales_bacterium_1_7_47faa_species_sam',
	'clostridiales_bacterium_bv3c26_species_sam',
	'clostridiales_family_xi_incertae_sedis_family_sam',
	'clostridiales_uncl_family_sam',
	'clostridium_asparagiforme_species_sam',
	'clostridium_bartlettii_species_sam',
	'clostridium_bolteae_species_sam',
	'clostridium_celatum_species_sam',
	'clostridium_citroniae_species_sam',
	'clostridium_clostridioforme_species_sam',
	'clostridium_difficile_species_sam',
	'clostridium_genus_sam',
	'clostridium_glycolicum_species_sam',
	'clostridium_hathewayi_species_sam',
	'clostridium_hylemonae_species_sam',
	'clostridium_innocuum_species_sam',
	'clostridium_leptum_species_sam',
	'clostridium_methylpentosum_species_sam',
	'clostridium_nexile_species_sam',
	'clostridium_perfringens_species_sam',
	'clostridium_ramosum_species_sam',
	'clostridium_scindens_species_sam',
	'clostridium_sp_kle_1755_species_sam',
	'clostridium_sp_l2_50_species_sam',
	'clostridium_symbiosum_species_sam',
	'collinsella_aerofaciens_species_sam',
	'collinsella_genus_sam',
	'collinsella_intestinalis_species_sam',
	'collinsella_tanakaei_species_sam',
	'collinsella_unclassified_species_sam',
	'comamonadaceae_family_sam',
	'comamonas_genus_sam',
	'comamonas_testosteroni_species_sam',
	'comamonas_unclassified_species_sam',
	'coprobacillus_genus_sam',
	'coprobacillus_sp_29_1_species_sam',
	'coprobacillus_unclassified_species_sam',
	'coprobacter_fastidiosus_species_sam',
	'coprococcus_catus_species_sam',
	'coprococcus_comes_species_sam',
	'coprococcus_eutactus_species_sam',
	'coprococcus_genus_sam',
	'coprococcus_sp_art55_1_species_sam',
	'coriobacteriaceae_bacterium_bv3ac1_species_sam',
	'coriobacteriaceae_bacterium_phi_species_sam',
	'coriobacteriaceae_family_sam',
	'corynebacteriaceae_family_sam',
	'corynebacterium_amycolatum_species_sam',
	'corynebacterium_aurimucosum_species_sam',
	'corynebacterium_durum_species_sam',
	'corynebacterium_genitalium_species_sam',
	'corynebacterium_genus_sam',
	'corynebacterium_glucuronolyticum_species_sam',
	'corynebacterium_jeikeium_species_sam',
	'corynebacterium_massiliense_species_sam',
	'corynebacterium_pseudogenitalium_species_sam',
	'corynebacterium_pyruviciproducens_species_sam',
	'corynebacterium_striatum_species_sam',
	'corynebacterium_tuberculostearicum_species_sam',
	'corynebacterium_urealyticum_species_sam',
	'cupriavidus_genus_sam',
	'cupriavidus_metallidurans_species_sam',
	'cupriavidus_unclassified_species_sam',
	'dasheen_mosaic_virus_species_sam',
	'days_post_adm_rf_sam',
	'days_post_adm_sam',
	'deinococcaceae_family_sam',
	'deinococcus_genus_sam',
	'deinococcus_unclassified_species_sam',
	'delftia_genus_sam',
	'desulfobulbaceae_family_sam',
	'desulfobulbaceae_unclassified_genus_sam',
	'desulfovibrio_desulfuricans_species_sam',
	'desulfovibrio_genus_sam',
	'desulfovibrio_piger_species_sam',
	'desulfovibrionaceae_family_sam',
	'dialister_genus_sam',
	'dialister_invisus_species_sam',
	'dialister_micraerophilus_species_sam',
	'dialister_succinatiphilus_species_sam',
	'dorea_formicigenerans_species_sam',
	'dorea_genus_sam',
	'dorea_longicatena_species_sam',
	'dorea_unclassified_species_sam',
	'dysgonomonas_gadei_species_sam',
	'dysgonomonas_unclassified_species_sam',
	'ectothiorhodospiraceae_family_sam',
	'eggerthella_genus_sam',
	'eggerthella_lenta_species_sam',
	'eggerthella_sp_1_3_56faa_species_sam',
	'eggerthella_unclassified_species_sam',
	'eggerthia_catenaformis_species_sam',
	'enhydrobacter_aerosaccus_species_sam',
	'enhydrobacter_genus_sam',
	'enterobacter_aerogenes_species_sam',
	'enterobacter_cloacae_species_sam',
	'enterobacter_genus_sam',
	'enterobacteria_phage_lambda_species_sam',
	'enterobacteria_phage_phix174_sensu_lato_species_sam',
	'enterobacteria_phage_rb69_species_sam',
	'enterobacteriaceae_bacterium_9_2_54faa_species_sam',
	'enterobacteriaceae_family_sam',
	'enterococcaceae_family_sam',
	'enterococcus_avium_species_sam',
	'enterococcus_casseliflavus_species_sam',
	'enterococcus_dispar_species_sam',
	'enterococcus_faecalis_species_sam',
	'enterococcus_faecium_species_sam',
	'enterococcus_gallinarum_species_sam',
	'enterococcus_genus_sam',
	'enterococcus_hirae_species_sam',
	'enterococcus_italicus_species_sam',
	'enterococcus_phage_phifl3a_species_sam',
	'enterococcus_raffinosus_species_sam',
	'enterococcus_saccharolyticus_species_sam',
	'epsilon15likevirus_unclassified_species_sam',
	'epsilonproteobacteria_uncl_family_sam',
	'epsilonproteobacteria_uncl_unclassified_genus_sam',
	'eremococcus_genus_sam',
	'eremothecium_unclassified_species_sam',
	'erysipelotrichaceae_bacterium_21_3_species_sam',
	'erysipelotrichaceae_bacterium_2_2_44a_species_sam',
	'erysipelotrichaceae_bacterium_3_1_53_species_sam',
	'erysipelotrichaceae_bacterium_5_2_54faa_species_sam',
	'erysipelotrichaceae_bacterium_6_1_45_species_sam',
	'erysipelotrichaceae_family_sam',
	'escherichia_coli_species_sam',
	'escherichia_genus_sam',
	'escherichia_unclassified_species_sam',
	'ethanoligenens_genus_sam',
	'eubacteriaceae_family_sam',
	'eubacterium_biforme_species_sam',
	'eubacterium_brachy_species_sam',
	'eubacterium_cylindroides_species_sam',
	'eubacterium_dolichum_species_sam',
	'eubacterium_eligens_species_sam',
	'eubacterium_genus_sam',
	'eubacterium_hallii_species_sam',
	'eubacterium_infirmum_species_sam',
	'eubacterium_limosum_species_sam',
	'eubacterium_ramulus_species_sam',
	'eubacterium_rectale_species_sam',
	'eubacterium_siraeum_species_sam',
	'eubacterium_sp_3_1_31_species_sam',
	'eubacterium_ventriosum_species_sam',
	'facklamia_hominis_species_sam',
	'facklamia_ignava_species_sam',
	'facklamia_languida_species_sam',
	'facklamia_unclassified_species_sam',
	'faecalibacterium_genus_sam',
	'faecalibacterium_prausnitzii_species_sam',
	'finegoldia_genus_sam',
	'finegoldia_magna_species_sam',
	'flavobacteriaceae_family_sam',
	'flavobacterium_genus_sam',
	'flavonifractor_plautii_species_sam',
	'frankia_genus_sam',
	'frankiaceae_family_sam',
	'fusobacteriaceae_family_sam',
	'fusobacterium_genus_sam',
	'fusobacterium_gonidiaformans_species_sam',
	'fusobacterium_mortiferum_species_sam',
	'fusobacterium_necrophorum_species_sam',
	'fusobacterium_nucleatum_species_sam',
	'fusobacterium_periodonticum_species_sam',
	'fusobacterium_ulcerans_species_sam',
	'fusobacterium_varium_species_sam',
	'gardnerella_genus_sam',
	'gardnerella_vaginalis_species_sam',
	'gemella_genus_sam',
	'gemella_haemolysans_species_sam',
	'gemella_morbillorum_species_sam',
	'gemella_sanguinis_species_sam',
	'gemella_unclassified_species_sam',
	'gordonibacter_genus_sam',
	'gordonibacter_pamelaeae_species_sam',
	'granulicatella_adiacens_species_sam',
	'granulicatella_elegans_species_sam',
	'granulicatella_genus_sam',
	'granulicatella_unclassified_species_sam',
	'granulicella_genus_sam',
	'haemophilus_genus_sam',
	'haemophilus_haemolyticus_species_sam',
	'haemophilus_parainfluenzae_species_sam',
	'hafnia_alvei_species_sam',
	'halobacteriales_unclassified_family_sam',
	'helcococcus_kunzii_species_sam',
	'holdemania_filiformis_species_sam',
	'holdemania_genus_sam',
	'holdemania_sp_ap2_species_sam',
	'holdemania_unclassified_species_sam',
	'hospduration_sam',
	'human_herpesvirus_1_species_sam',
	'human_herpesvirus_4_species_sam',
	'human_papillomavirus_132_like_viruses_species_sam',
	'human_papillomavirus_species_sam',
	'human_papillomavirus_type_135_species_sam',
	'jc_polyomavirus_species_sam',
	'jonquetella_anthropi_species_sam',
	'jonquetella_genus_sam',
	'jonquetella_sp_bv3c21_species_sam',
	'jonquetella_unclassified_species_sam',
	'kingella_genus_sam',
	'klebsiella_genus_sam',
	'klebsiella_oxytoca_species_sam',
	'klebsiella_pneumoniae_species_sam',
	'klebsiella_unclassified_species_sam',
	'lachnospiraceae_bacterium_1_1_57faa_species_sam',
	'lachnospiraceae_bacterium_1_4_56faa_species_sam',
	'lachnospiraceae_bacterium_2_1_46faa_species_sam',
	'lachnospiraceae_bacterium_2_1_58faa_species_sam',
	'lachnospiraceae_bacterium_3_1_46faa_species_sam',
	'lachnospiraceae_bacterium_3_1_57faa_ct1_species_sam',
	'lachnospiraceae_bacterium_5_1_57faa_species_sam',
	'lachnospiraceae_bacterium_5_1_63faa_species_sam',
	'lachnospiraceae_bacterium_6_1_63faa_species_sam',
	'lachnospiraceae_bacterium_7_1_58faa_species_sam',
	'lachnospiraceae_bacterium_8_1_57faa_species_sam',
	'lachnospiraceae_bacterium_9_1_43bfaa_species_sam',
	'lachnospiraceae_family_sam',
	'lactobacillaceae_family_sam',
	'lactobacillus_amylovorus_species_sam',
	'lactobacillus_animalis_species_sam',
	'lactobacillus_antri_species_sam',
	'lactobacillus_brevis_species_sam',
	'lactobacillus_casei_paracasei_species_sam',
	'lactobacillus_crispatus_species_sam',
	'lactobacillus_curvatus_species_sam',
	'lactobacillus_delbrueckii_species_sam',
	'lactobacillus_fermentum_species_sam',
	'lactobacillus_gasseri_species_sam',
	'lactobacillus_genus_sam',
	'lactobacillus_helveticus_species_sam',
	'lactobacillus_iners_species_sam',
	'lactobacillus_jensenii_species_sam',
	'lactobacillus_johnsonii_species_sam',
	'lactobacillus_oris_species_sam',
	'lactobacillus_pentosus_species_sam',
	'lactobacillus_phage_lc_nu_species_sam',
	'lactobacillus_phage_pl_1_species_sam',
	'lactobacillus_plantarum_species_sam',
	'lactobacillus_reuteri_species_sam',
	'lactobacillus_rhamnosus_species_sam',
	'lactobacillus_ruminis_species_sam',
	'lactobacillus_sakei_species_sam',
	'lactobacillus_salivarius_species_sam',
	'lactobacillus_sanfranciscensis_species_sam',
	'lactobacillus_ultunensis_species_sam',
	'lactobacillus_vaginalis_species_sam',
	'lactococcus_genus_sam',
	'lactococcus_lactis_species_sam',
	'lactococcus_phage_jm2_species_sam',
	'lactococcus_raffinolactis_species_sam',
	'laribacter_genus_sam',
	'lautropia_genus_sam',
	'lautropia_mirabilis_species_sam',
	'leifsonia_unclassified_species_sam',
	'leptotrichales_unclassified_family_sam',
	'leuconostoc_citreum_species_sam',
	'leuconostoc_fallax_species_sam',
	'leuconostoc_genus_sam',
	'leuconostoc_lactis_species_sam',
	'leuconostoc_mesenteroides_species_sam',
	'leuconostoc_unclassified_species_sam',
	'leuconostocaceae_family_sam',
	'limnohabitans_unclassified_species_sam',
	'lymphocryptovirus_unclassified_species_sam',
	'malassezia_globosa_species_sam',
	'marinomonas_genus_sam',
	'marvinbryantia_formatexigens_species_sam',
	'marvinbryantia_genus_sam',
	'megamonas_funiformis_species_sam',
	'megamonas_genus_sam',
	'megamonas_hypermegale_species_sam',
	'megamonas_rupellensis_species_sam',
	'megamonas_unclassified_species_sam',
	'megasphaera_elsdenii_species_sam',
	'megasphaera_genomosp_type_1_species_sam',
	'megasphaera_genus_sam',
	'megasphaera_micronuciformis_species_sam',
	'megasphaera_sp_bv3c16_1_species_sam',
	'megasphaera_unclassified_species_sam',
	'merkel_cell_polyomavirus_species_sam',
	'methanobacteriaceae_family_sam',
	'methanobrevibacter_genus_sam',
	'methanobrevibacter_smithii_species_sam',
	'methanobrevibacter_unclassified_species_sam',
	'methanosphaera_genus_sam',
	'methanosphaera_stadtmanae_species_sam',
	'methylobacteriaceae_family_sam',
	'methylobacterium_extorquens_species_sam',
	'methylobacterium_genus_sam',
	'methylocystaceae_family_sam',
	'methylocystaceae_unclassified_genus_sam',
	'micrococcaceae_family_sam',
	'mitsuokella_genus_sam',
	'mitsuokella_multacida_species_sam',
	'mitsuokella_unclassified_species_sam',
	'mobiluncus_curtisii_species_sam',
	'mobiluncus_genus_sam',
	'mobiluncus_unclassified_species_sam',
	'moraxellaceae_family_sam',
	'morganella_morganii_species_sam',
	'mycoplasma_genus_sam',
	'mycoplasma_hominis_species_sam',
	'mycoplasmataceae_family_sam', # mycoplasmatacea do not share a phylum with mycobacteria
	'naumovozyma_unclassified_species_sam',
	'neisseriaceae_family_sam',
	'neisseriaceae_unclassified_genus_sam',
	'oceanospirillaceae_family_sam',
	'ochrobactrum_anthropi_species_sam',
	'ochrobactrum_genus_sam',
	'odoribacter_genus_sam',
	'odoribacter_laneus_species_sam',
	'odoribacter_splanchnicus_species_sam',
	'odoribacter_unclassified_species_sam',
	'olsenella_genus_sam',
	'olsenella_uli_species_sam',
	'olsenella_unclassified_species_sam',
	'oribacterium_genus_sam',
	'oribacterium_sinus_species_sam',
	'oribacterium_sp_oral_taxon_078_species_sam',
	'orthohepadnavirus_unclassified_species_sam',
	'oscillatoriaceae_family_sam',
	'oscillatoriaceae_unclassified_genus_sam',
	'oscillibacter_sp_kle_1745_species_sam',
	'oscillibacter_unclassified_species_sam',
	'oxalobacter_formigenes_species_sam',
	'oxalobacter_genus_sam',
	'oxalobacteraceae_family_sam',
	'paenibacillus_dendritiformis_species_sam',
	'pantoea_genus_sam',
	'pantoea_unclassified_species_sam',
	'parabacteroides_distasonis_species_sam',
	'parabacteroides_genus_sam',
	'parabacteroides_goldsteinii_species_sam',
	'parabacteroides_johnsonii_species_sam',
	'parabacteroides_merdae_species_sam',
	'parabacteroides_unclassified_species_sam',
	'paraprevotella_clara_species_sam',
	'paraprevotella_unclassified_species_sam',
	'paraprevotella_xylaniphila_species_sam',
	'parascardovia_genus_sam',
	'parasutterella_excrementihominis_species_sam',
	'parvimonas_genus_sam',
	'parvimonas_micra_species_sam',
	'parvimonas_unclassified_species_sam',
	'parvovirus_nih_cqv_species_sam',
	'pasteurellaceae_family_sam',
	'pectobacterium_genus_sam',
	'pectobacterium_wasabiae_species_sam',
	'pediococcus_acidilactici_species_sam',
	'pediococcus_genus_sam',
	'pediococcus_lolii_species_sam',
	'pediococcus_pentosaceus_species_sam',
	'peptoniphilus_duerdenii_species_sam',
	'peptoniphilus_genus_sam',
	'peptoniphilus_harei_species_sam',
	'peptoniphilus_lacrimalis_species_sam',
	'peptoniphilus_rhinitidis_species_sam',
	'peptoniphilus_sp_bv3ac2_species_sam',
	'peptoniphilus_sp_jc140_species_sam',
	'peptoniphilus_sp_oral_taxon_375_species_sam',
	'peptoniphilus_timonensis_species_sam',
	'peptostreptococcaceae_family_sam',
	'peptostreptococcaceae_noname_unclassified_species_sam',
	'peptostreptococcus_anaerobius_species_sam',
	'peptostreptococcus_genus_sam',
	'peptostreptococcus_stomatis_species_sam',
	'peptostreptococcus_unclassified_species_sam',
	'phascolarctobacterium_genus_sam',
	'phascolarctobacterium_succinatutens_species_sam',
	'phicd119likevirus_unclassified_species_sam',
	'polyomavirus_hpyv6_species_sam',
	'polyomavirus_hpyv7_species_sam',
	'porcine_type_c_oncovirus_species_sam',
	'porphyromonadaceae_family_sam',
	'porphyromonas_asaccharolytica_species_sam',
	'porphyromonas_bennonis_species_sam',
	'porphyromonas_genus_sam',
	'porphyromonas_somerae_species_sam',
	'porphyromonas_uenonis_species_sam',
	'prevotella_amnii_species_sam',
	'prevotella_bergensis_species_sam',
	'prevotella_bivia_species_sam',
	'prevotella_buccae_species_sam',
	'prevotella_buccalis_species_sam',
	'prevotella_copri_species_sam',
	'prevotella_disiens_species_sam',
	'prevotella_genus_sam',
	'prevotella_histicola_species_sam',
	'prevotella_loescheii_species_sam',
	'prevotella_melaninogenica_species_sam',
	'prevotella_oris_species_sam',
	'prevotella_paludivivens_species_sam',
	'prevotella_stercorea_species_sam',
	'prevotella_timonensis_species_sam',
	'prevotellaceae_family_sam',
	'propionibacteriaceae_family_sam',
	'propionibacterium_acnes_species_sam',
	'propionibacterium_avidum_species_sam',
	'propionibacterium_freudenreichii_species_sam',
	'propionibacterium_genus_sam',
	'propionibacterium_granulosum_species_sam',
	'propionibacterium_phage_p104a_species_sam',
	'propionibacterium_phage_pad20_species_sam',
	'propionimicrobium_lymphophilum_species_sam',
	'proteus_genus_sam',
	'proteus_mirabilis_species_sam',
	'proteus_penneri_species_sam',
	'proteus_unclassified_species_sam',
	'pseudoflavonifractor_capillosus_species_sam',
	'pseudoflavonifractor_genus_sam',
	'pseudomonadaceae_family_sam',
	'pseudomonas_aeruginosa_species_sam',
	'pseudomonas_fragi_species_sam',
	'pseudomonas_genus_sam',
	'pseudomonas_phage_d3112_species_sam',
	'pseudomonas_phage_f116_species_sam',
	'pseudomonas_unclassified_species_sam',
	'pseudoramibacter_alactolyticus_species_sam',
	'pseudoramibacter_genus_sam',
	'pyramidobacter_genus_sam',
	'pyramidobacter_piscolens_species_sam',
	'ralstonia_genus_sam',
	'ralstonia_unclassified_species_sam',
	'rhodopseudomonas_genus_sam',
	'rhodopseudomonas_palustris_species_sam',
	'rikenellaceae_family_sam',
	'roseburia_genus_sam',
	'roseburia_hominis_species_sam',
	'roseburia_intestinalis_species_sam',
	'roseburia_inulinivorans_species_sam',
	'roseburia_unclassified_species_sam',
	'rothia_dentocariosa_species_sam',
	'rothia_genus_sam',
	'rothia_mucilaginosa_species_sam',
	'rothia_unclassified_species_sam',
	'rubrobacter_genus_sam',
	'rubrobacteraceae_family_sam',
	'ruminococcaceae_bacterium_d16_species_sam',
	'ruminococcaceae_family_sam',
	'ruminococcus_bromii_species_sam',
	'ruminococcus_callidus_species_sam',
	'ruminococcus_champanellensis_species_sam',
	'ruminococcus_genus_sam',
	'ruminococcus_gnavus_species_sam',
	'ruminococcus_lactaris_species_sam',
	'ruminococcus_obeum_species_sam',
	'ruminococcus_sp_5_1_39bfaa_species_sam',
	'ruminococcus_torques_species_sam',
	'saccharomyces_cerevisiae_species_sam',
	'salmonella_genus_sam',
	'salmonella_phage_hk620_species_sam',
	'scardovia_unclassified_species_sam',
	'scardovia_wiggsiae_species_sam',
	'sebaldella_genus_sam',
	'sebaldella_termitidis_species_sam',
	'sebaldellaceae_family_sam',
	'selenomonas_genus_sam',
	'selenomonas_noxia_species_sam',
	'serratia_marcescens_species_sam',
	'shigella_genus_sam',
	'shigella_phage_sf6_species_sam',
	'shigella_sonnei_species_sam'
	'shuttleworthia_genus_sam', 
	'slackia_exigua_species_sam', 
	'slackia_genus_sam', 
	'slackia_piriformis_species_sam', 
	'slackia_unclassified_species_sam', 
	'solobacterium_genus_sam', 
	'solobacterium_moorei_species_sam', 
	'sphingobacteriaceae_family_sam', 
	'sphingobacteriaceae_unclassified_genus_sam', 
	'sphingobacterium_unclassified_species_sam', 
	'staphylococcaceae_family_sam', 
	'staphylococcus_aureus_species_sam', 
	'staphylococcus_caprae_capitis_species_sam', 
	'staphylococcus_epidermidis_species_sam', 
	'staphylococcus_genus_sam', 
	'staphylococcus_haemolyticus_species_sam', 
	'staphylococcus_hominis_species_sam', 
	'staphylococcus_lugdunensis_species_sam', 
	'staphylococcus_phage_pvl_species_sam', 
	'staphylococcus_phage_rosa_species_sam', 
	'staphylococcus_simulans_species_sam', 
	'stenotrophomonas_genus_sam', 
	'stenotrophomonas_maltophilia_species_sam', 
	'stenotrophomonas_unclassified_species_sam', 
	'stomatobaculum_longum_species_sam', 
	'streptococcaceae_family_sam', 
	'streptococcus_agalactiae_species_sam', 
	'streptococcus_anginosus_species_sam', 
	'streptococcus_australis_species_sam', 
	'streptococcus_constellatus_species_sam', 
	'streptococcus_cristatus_species_sam', 
	'streptococcus_dysgalactiae_species_sam', 
	'streptococcus_gallolyticus_species_sam', 
	'streptococcus_genus_sam', 
	'streptococcus_gordonii_species_sam', 
	'streptococcus_infantarius_species_sam', 
	'streptococcus_infantis_species_sam', 
	'streptococcus_intermedius_species_sam', 
	'streptococcus_lutetiensis_species_sam', 
	'streptococcus_macedonicus_species_sam', 
	'streptococcus_mitis_oralis_pneumoniae_species_sam', 
	'streptococcus_mutans_species_sam', 
	'streptococcus_orisratti_species_sam', 
	'streptococcus_parasanguinis_species_sam', 
	'streptococcus_parauberis_species_sam', 
	'streptococcus_pasteurianus_species_sam', 
	'streptococcus_pyogenes_species_sam', 
	'streptococcus_salivarius_species_sam', 
	'streptococcus_sanguinis_species_sam', 
	'streptococcus_thermophilus_species_sam', 
	'streptococcus_tigurinus_species_sam', 
	'streptococcus_vestibularis_species_sam', 
	'subdoligranulum_genus_sam', 
	'subdoligranulum_sp_4_3_54a2faa_species_sam', 
	'subdoligranulum_unclassified_species_sam', 
	'sutterella_genus_sam', 
	'sutterella_wadsworthensis_species_sam', 
	'sutterellaceae_family_sam', 
	'synergistaceae_family_sam', 
	't5likevirus_unclassified_species_sam', 
	'thermus_scotoductus_species_sam', 
	'thioalkalivibrio_genus_sam', 
	'thiomonas_unclassified_species_sam', 
	'tobacco_vein_clearing_virus_species_sam', 
	'torque_teno_virus_3_species_sam', 
	'torque_teno_virus_species_sam', 
	'turicibacter_genus_sam', 
	'turicibacter_sanguinis_species_sam', 
	'turicibacter_unclassified_species_sam', 
	'ureaplasma_genus_sam', 
	'ureaplasma_parvum_species_sam', 
	'ureaplasma_unclassified_species_sam', 
	'varibaculum_cambriense_species_sam', 
	'veillonella_atypica_species_sam', 
	'veillonella_dispar_species_sam', 
	'veillonella_genus_sam', 
	'veillonella_parvula_species_sam', 
	'veillonella_ratti_species_sam', 
	'veillonella_unclassified_species_sam', 
	'veillonellaceae_family_sam', 
	'verrucomicrobiaceae_family_sam', 
	'victivallaceae_family_sam', 
	'victivallis_genus_sam', 
	'viunalikevirus_unclassified_species_sam', 
	'weissella_confusa_species_sam', 
	'weissella_unclassified_species_sam',
	'xanthomonadaceae_family_sam', 
	'xanthomonas_genus_sam', 
	'yersinia_genus_sam', 
	'yersinia_phage_l_413c_species_sam'
]