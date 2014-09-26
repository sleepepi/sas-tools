*Check if dataset exists and, if missing, create empty dataset with:
  variable &id_varname, format of &macrostore_idformat, with missing values in that format represented by &macrostore_idmisval;
*Code adapted from that written by Spencer Childress and Brandon Welch of SAS Institute: http://analytics.ncsu.edu/sesug/2011/CC19.Childress.pdf;

%macro create_emptydataset_ifmissing(dataset2check, id_varname, macrostore_idformat=best12., macrostore_idmisval=.);

 %local Exist NumObs;
 %let Exist = No;
 %let NumObs = 0;

 %if %sysfunc(exist(&dataset2check)) %then %let Exist = Yes;

 %if &Exist = Yes %then %do;
 %let DSNId = %sysfunc(open(&dataset2check));
 %let DSObs = %sysfunc(attrn(&DSNId.,nobs));
 %let rc = %sysfunc(close(&DSNId.));
 %let NumObs = &DSObs.;
 %end;

 %put;
 %put **************** Check for Existence of &dataset2check *******************;
 %put EXIST: &Exist;
 %put NUMBER OF OBS: &NumObs;
 %put **************************************************************************;

  %if &Exist = No or &NumObs = 0 %then %do;
    DATA &dataset2check;
      format &id_varname &macrostore_idformat;
      &id_varname = &macrostore_idmisval;
    RUN;
  %end;

%mend create_emptydataset_ifmissing;

*count the number of words in a list;
 *Code adopted from Robert J. Morris of SAS Institute: http://www2.sas.com/proceedings/sugi30/029-30.pdf;
%macro num_tokens(words, delim=%str( ));
 %local counter;

 %* Loop through the words list, incrementing a counter for each word found. ;
 %let counter = 1;
 %do %while (%length(%scan(&words, &counter, &delim)) > 0);
 %let counter = %eval(&counter + 1);
 %end;

 %* Our loop above pushes the counter past the number of words by 1. ;
 %let counter = %eval(&counter - 1);

 %* Output the count of the number of words. ;
 &counter

%mend num_tokens;

*Add a string constant as a suffix or prefix to all words in a list;
 *Code adopted from Robert J. Morris of SAS Institute: http://www2.sas.com/proceedings/sugi30/029-30.pdf;
%macro add_string(words, str, delim=%str( ), location=suffix);
 %local outstr i word num_words;

 %* Verify macro arguments. ;
 %if (%length(&words) eq 0) %then %do;
 %put ***ERROR(add_string): Required argument 'words' is missing.;
 %goto exit;
 %end;
 %if (%length(&str) eq 0) %then %do;
 %put ***ERROR(add_string): Required argument 'str' is missing.;
 %goto exit;
 %end;
 %if (%upcase(&location) ne SUFFIX and %upcase(&location) ne PREFIX) %then %do;
 %put ***ERROR(add_string): Optional argument 'location' must be;
 %put *** set to SUFFIX or PREFIX.;
 %goto exit;
 %end;

 %* Build the outstr by looping through the words list and adding the
 * requested string onto each word. ;
 %let outstr = ;
 %let num_words = %num_tokens(&words, delim=&delim);
 %do i=1 %to &num_words;
 %let word = %scan(&words, &i, &delim);
 %if (&i eq 1) %then %do;
 %if (%upcase(&location) eq PREFIX) %then %do;
 %let outstr = &str&word;
 %end;
 %else %do;
 %let outstr = &word&str;
 %end;
 %end;
 %else %do;
 %if (%upcase(&location) eq PREFIX) %then %do;
 %let outstr = &outstr&delim&str&word;
 %end;
 %else %do;
 %let outstr = &outstr&delim&word&str;
 %end;
 %end;
 %end;

 %* Output the new list of words. ;
 &outstr

 %exit:
%mend add_string;

*Join each item in one list of words to an item in another list of words (sequential matching);
 *Code adopted from Robert J. Morris of SAS institute: http://www2.sas.com/proceedings/sugi30/029-30.pdf;
%macro parallel_join(words1, words2, joinstr, delim1=%str( ), delim2=%str( ));
 %local i num_words1 num_words2 word outstr;

 %* Verify macro arguments. ;
 %if (%length(&words1) eq 0) %then %do;
 %put ***ERROR(parallel_join): Required argument 'words1' is missing.;
 %goto exit;
 %end;
 %if (%length(&words2) eq 0) %then %do;
 %put ***ERROR(parallel_join): Required argument 'words2' is missing.;
 %goto exit;
 %end;
 %if (%length(&joinstr) eq 0) %then %do;
 %put ***ERROR(parallel_join): Required argument 'joinstr' is missing.;
 %goto exit;
 %end;

 %* Find the number of words in each list. ;
 %let num_words1 = %num_tokens(&words1, delim=&delim1);
 %let num_words2 = %num_tokens(&words2, delim=&delim2);

 %* Check the number of words. ;
 %if (&num_words1 ne &num_words2) %then %do;
 %put ***ERROR(parallel_join): The number of words in 'words1' and;
 %put *** 'words2' must be equal.;
 %goto exit;
 %end;

 %* Build the outstr by looping through the corresponding words and joining
 * them by the joinstr. ;
 %let outstr=;
 %do i = 1 %to &num_words1;
 %let word = %scan(&words1, &i, &delim1);
 %let outstr = &outstr &word&joinstr%scan(&words2, &i, &delim2);
 %end;

 %* Output the list of joined words. ;
 &outstr

 %exit:
%mend parallel_join;

*Rename a list of variable en masse by adding a constant string as a prefix or suffix;
 *Code adopted from Robert J. Morris of SAS institute: http://www2.sas.com/proceedings/sugi30/029-30.pdf;
%macro rename_string(words, str, delim=%str( ), location=suffix);

 %* Verify macro arguments. ;
 %if (%length(&words) eq 0) %then %do;
 %put ***ERROR(rename_string): Required argument 'words' is missing.;
 %goto exit;
 %end;
 %if (%length(&str) eq 0) %then %do;
 %put ***ERROR(rename_string): Required argument 'str' is missing.;
 %goto exit;
 %end;
 %if (%upcase(&location) ne SUFFIX and %upcase(&location) ne PREFIX) %then %do;
 %put ***ERROR(rename_string): Optional argument 'location' must be;
 %put *** set to SUFFIX or PREFIX.;
 %goto exit;
 %end;

 %* Since rename_string is just a special case of parallel_join,
 * simply pass the appropriate arguments on to that macro. ;
 %parallel_join(
 &words,
 %add_string(&words, &str, delim=&delim, location=&location),
 =,
 delim1 = &delim,
 delim2 = &delim
 )

 %exit:
%mend rename_string;

*Create list of words appending an iterative numeric suffix to the end of a base word;
 *Code adopted from Robert J. Morris of SAS institute: http://www2.sas.com/proceedings/sugi30/029-30.pdf;
%macro suffix_counter(base, end, start=1, zpad=0);
 %local outstr i counter;

 %* Verify macro arguments. ;
 %if (%length(&base) eq 0) %then %do;
 %put ***ERROR(suffix_counter): Required argument 'base' is missing.;
 %goto exit;
 %end;
 %if (%length(&end) eq 0) %then %do;
 %put ***ERROR(suffix_counter): Required argument 'end' is missing.;
 %goto exit;
 %end;
 %if (&end < &start) %then %do;
 %put ***ERROR(suffix_counter): The 'end' argument must not be less;
 %put *** than the 'start' argument.;
 %goto exit;
 %end;

 %* Construct the outstr by looping from &start to &end, adding the counter
 * value to &base in each iteration. To handle the zero-padding, use the
 * putn function to format the counter variable with the Z. format. ;
 %let outstr=;
 %do i=&start %to &end;
 %if (&zpad > 0) %then %do;
 %let counter = %sysfunc(putn(&i, z&zpad..));
 %end;
 %else %do;
 %let counter = &i;
 %end;
 %let outstr=&outstr &base&counter;
 %end;

 %* Output the new list. ;
 &outstr

 %exit:
%mend suffix_counter;
