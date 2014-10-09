%let username = ;

%let rootpath = C:\Users;
%let sas_tools_path = sas-tools;

*include macros developed by the SAS institute that are called by sleepepi's custom macros;
%include "&rootpath\&username\&sas_tools_path\sas-institute-macros.sas";

**************************************************************************************************************************************************;
* Compare Datasets to Identify Conflicting Variable Labels (potentially indicating that the variables contain different information)
**************************************************************************************************************************************************;

* Generate contents for dataset, appending a prefix or suffix to dataset name;
%macro generate_contents(datasetname, append = _contents, location = suffix, inlibrary = work, outlibrary = work, overwrite = NO);
  %local in_datasetname contents_datasetname;

  %* Verify Arguments;
  %if (%upcase(&overwrite) ne NO and %upcase(&overwrite) ne YES) %then %do;
    %put ********************************** ERROR **************************************;
    %put %str(*)%str( ) ERROR(generate_contents): Optional argument 'overwrite' must be set to YES or NO;
    %put *******************************************************************************;
    %goto exit;
  %end;

  %let in_datasetname = %parallel_join(words1=&inlibrary,words2=&datasetname,joinstr=.);
  %let contents_datasetname = %parallel_join(&outlibrary, %add_string(&datasetname, &append, location = &location),joinstr=.);

  %* Check for existence of dataset with same name as output dataset;
  %* Only overwrite if designated by argument;
  %if %sysfunc(exist(&contents_datasetname)) %then %do;
    %if %upcase(&overwrite) = NO %then %do;
        %put ********************* ERROR: INTENDED DATASET NAME ALREADY EXISTS *********************;
        %put %str(*)%str( ) ERROR(generate_contents): &contents_datasetname exists. To create contents, either:;
        %put %str(*)%str( )%str( )%str( ) 1. Change prefix or suffix;
        %put %str(*)%str( )%str( )%str( ) 2. Change 'overwrite' option to 'YES';
        %put %str(*)%str( ) "Contents" dataset named &contents_datasetname was not created.;
        %put ***************************************************************************************;
        %goto exit;
    %end;
    %else %do;
        %put ********************************** WARNING **************************************;
        %put %str(*)%str( ) WARNING(generate_contents): &contents_datasetname existed before macro call.;
        %put %str(*)%str( ) Previous dataset named &contents_datasetname was OVERWRITTEN.;
        %put *********************************************************************************;
    %end;
  %end;

  proc contents data = &in_datasetname noprint out = &contents_datasetname;
  run;

  proc sort data = &contents_datasetname;
  by name;
  run;

  %exit:
%mend;

* Generate contents for several datasets whose names are stored in a list, separated by "delim";
%macro many_contents(dataset_list, append = _contents, location = suffix, delim =%str( ), inlibrary = work, outlibrary = work, overwrite = NO);
  %local i num_datasets;

  %*loop through generate_contents() macro for all datasets contained in list;
  %let num_datasets = %num_tokens(&dataset_list, delim=&delim);
  %do i = 1 %to &num_datasets;
    %let current_dataset = %scan(&dataset_list, &i, &delim);
    %generate_contents(datasetname = &current_dataset, append = &append, location = &location, inlibrary = &inlibrary, outlibrary = &outlibrary, overwrite = &overwrite);
  %end;

%mend;

* Generate contents for several datasets stored in a list;
* Create output datasets listing all contents ('all_contents'), variable names used multiple times ('all_contents_multiuse_names'), and potential conflicts ('check_multiuse_names');
%macro create_contents_note_conflicts(dataset_list, append = _contents, location = suffix, delim = %str( ), inlibrary = work, outlibrary = work, overwrite = NO, delete_indivsets = YES);
  %local combined_contents_list variables_to_check sets_to_delete;

  %* Verify Arguments;
  %if (%upcase(&delete_indivsets) ne NO and %upcase(&delete_indivsets) ne YES) %then %do;
    %put ********************************** ERROR **************************************;
    %put ***ERROR(create_contents_note_conflicts): Optional argument 'delete_indivsets' must be set to YES or NO;
    %put *******************************************************************************;
    %goto exit;
  %end;

  %many_contents(&dataset_list, append = &append, location = &location, delim = &delim, inlibrary = &inlibrary, outlibrary = &outlibrary, overwrite = &overwrite);
  %let combined_contents_list = %add_string(words=&dataset_list, str = &append, delim = &delim, location = &location);

  %*Generate output datasets;
  data all_contents;
    set &combined_contents_list;
  run;

  proc sql noprint;
    %*Create dataset listing all the variables used multiple times;
    create table all_contents_multiuse_names as
    select *
    from all_contents
    group by name
      having count(*) ge 2
    order by name;

    create table multiuse_names_unique_labels as
    select distinct(label), name
    from all_contents_multiuse_names
    group by name
      having count(*) ge 1
    order by name;

    select distinct(quote(strip(name))) into :variables_to_check separated by ', '
    from multiuse_names_unique_labels
    group by name
      having count(*) > 1;

    %*Identify cases where labels for different iterations of variable names conflict;
    create table check_multiuse_names as
    select *
    from all_contents
    where name in (&variables_to_check)
    order by name;
  quit;

  %if %upcase(&delete_indivsets) = YES %then %do;
    %let sets_to_delete = &combined_contents_list;
  %end;

  %*Delete excess datasets to conserve storage space;
  proc datasets nolist;
    delete multiuse_names_unique_labels &sets_to_delete;
  run;
  quit;

  %exit:
%mend;

**************************************************************************************************************************************************;
* Sample Call to create_contents_note_conflicts();
* The following call creates contents datasets for dataset1-dataset5 from 'samplib' library, merges their contents into one file, checks for
        the same variable name being shared across datasets, and identifies variables where labels conflict:
* %let list_of_merged_datasets = dataset1 dataset2 dataset3 dataset4 dataset5;
* %create_contents_note_conflicts(&list_of_merged_datasets, inlibrary = samplib, delete_indivsets=NO);
**************************************************************************************************************************************************;


**************************************************************************************************************************************************;
* Identify variables that may need to undergo QC
**************************************************************************************************************************************************;
* Identify variables that contain a certain value;
* Purpose: some datasets use particular values (e.g., -1, -9) to signify data that is missing or N/A;
* &variable_list is a list of variables to check separated by &delim, and &value is the value to search;
%macro list_vars_containing_value(dataset, variable_list, value, delim = %str( ));
  %local output_list i current_var num_vars contains_value;

  %* Verify macro arguments. ;
  %if (%length(&variable_list) eq 0) %then %do;
    %put ***ERROR(add_string): Required argument 'variable_list' is missing.;
    %goto exit;
  %end;

  %* Build the output_list by looping through the variable_list list and check if the variable ever contains the value;
  %let output_list = ;
  %let num_vars = %num_tokens(&variable_list, delim=&delim);

  %do i=1 %to &num_vars;
    %let current_var = %scan(&variable_list, &i, &delim);
    %let contains_value = 0;
    %do;
      proc sql noprint;
        select exists(select * from &dataset where &current_var = &value) into :contains_value
        from &dataset;
      quit;
    %end;
    %if &contains_value = 1 %then %do;
      %if (%length(&output_list) eq 0) %then %do;
        %let output_list = &current_var;
      %end;
      %else %do;
        %let output_list = &output_list&delim&current_var;
      %end;
    %end;
  %end;

   %* Output the list of variables contain the value of interest to the SAS Log. ;
   %put &output_list;

  %exit:
%mend list_vars_containing_value;

* Identify variables that contain only integers (no floating point numbers);
* Purpose: to aid in identifying variables that might be numeric representations of categorical variables;
* &variable_list is a list of variables to check separated by &delim;
%macro list_integer_vars(dataset, variable_list, delim = %str( ));
  %local output_list i current_var num_vars is_float;

  %* Verify macro arguments. ;
  %if (%length(&variable_list) eq 0) %then %do;
    %put ***ERROR(add_string): Required argument 'variable_list' is missing.;
    %goto exit;
  %end;

  %* Build the output_list by looping through the variable_list list and check if the variables are all integers;
  %let output_list = ;
  %let num_vars = %num_tokens(&variable_list, delim=&delim);

  %do i=1 %to &num_vars;
    %let current_var = %scan(&variable_list, &i, &delim);
    %let is_float = 0;
    %do;
      proc sql noprint;
        select exists(select * from &dataset where mod(&current_var,1) not in (0,.)) into :is_float
        from &dataset;
      quit;
    %end;
    %if &is_float = 0 %then %do;
      %if (%length(&output_list) eq 0) %then %do;
        %let output_list = &current_var;
      %end;
      %else %do;
        %let output_list = &output_list&delim&current_var;
      %end;
    %end;
  %end;

   %* Output the list of variables containing only integers to the SAS Log. ;
   %put &output_list;

  %exit:
%mend list_integer_vars;
