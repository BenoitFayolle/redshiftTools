#' Move redshift table from one cluster to another
#'
#' Unload a table to S3 and then load it with redshift, replacing the contents of that table.
#' The table on redshift has to have the same structure and column ordering to work correctly.
#'
#' @param table_name the name of the table to move
#' @param schema_from the name of the schema where the source table is
#' @param schema_to the name of the destination schema
#' @param dbcon_from a db connection to the source RS cluster
#' @param dbcon_to a db connection to the destination RS cluster
#' @param split_files optional parameter to specify amount of files to split into. If not specified will look at amount of slices in Redshift to determine an optimal amount.
#' @param bucket the name of the temporary bucket to load the data. Will look for AWS_BUCKET_NAME on environment if not specified.
#' @param region the region of the bucket. Will look for AWS_DEFAULT_REGION on environment if not specified.
#' @param access_key the access key with permissions for the bucket. Will look for AWS_ACCESS_KEY_ID on environment if not specified.
#' @param secret_key the secret key with permissions fot the bucket. Will look for AWS_SECRET_ACCESS_KEY on environment if not specified.
#' @param session_token the session key with permissions for the bucket, this will be used instead of the access/secret keys if specified. Will look for AWS_SESSION_TOKEN on environment if not specified.
#' @param iam_role_arn an iam role arn with permissions fot the bucket. Will look for AWS_IAM_ROLE_ARN on environment if not specified. This is ignoring access_key and secret_key if set.
#' @param wlm_slots amount of WLM slots to use for this bulk load http://docs.aws.amazon.com/redshift/latest/dg/tutorial-configuring-workload-management.html
#' @param additional_params Additional params to send to the COPY statement in Redshift
#'
#' @examples
#' library(DBI)
#'
#' a=data.frame(a=seq(1,10000), b=seq(10000,1))
#'
#'\dontrun{
#' dbcon_from <- dbConnect(RPostgres::Postgres(), dbname="dbname",
#' host='my-redshift-url.amazon.com', port='5439',
#' user='myuser', password='mypassword',sslmode='require')
#'  dbcon_to <- dbConnect(RPostgres::Postgres(), dbname="dbname2",
#' host='my-redshift-url2.amazon.com', port='5439',
#' user='myuser', password='mypassword2',sslmode='require')

#'
#' rs_move_table(table_name,"public","public",dbcon_from,dbcom_to,
#' bucket="my-bucket", split_files=4)
#'
#' }
#' @export
rs_move_table = function(
  table_name,
  schema_from,
  schema_to,
  dbcon_from,
  dbcon_to,
  split_files,
  bucket=Sys.getenv('AWS_BUCKET_NAME'),
  region=Sys.getenv('AWS_DEFAULT_REGION'),
  access_key=Sys.getenv('AWS_ACCESS_KEY_ID'),
  secret_key=Sys.getenv('AWS_SECRET_ACCESS_KEY'),
  session_token=Sys.getenv('AWS_SESSION_TOKEN'),
  iam_role_arn=Sys.getenv('AWS_IAM_ROLE_ARN'),
  wlm_slots=1,
  additional_params=''){

  message('Checking table exists in destination')

  svv_to <- dbGetQuery(dbcon_to,glue("select * from svv_table_info where schema = '{schema_to}' and \"table\" = '{table_name}'"))

  if(nrow(svv_to) != 1){
    message("Table doesn't exist in destination. Creating..")
    ddl <- dbGetQuery(dbcon_from,glue("select ddl from admin.v_generate_tbl_ddl where tablename = '{table_name}'  and schemaname = '{schema_from}'"))
    if(nrow(ddl) == 0){
      warning("ddl not found. Please generate it with the amazon-redshift-utils command.
              See https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_generate_tbl_ddl.sql")
      return(FALSE)
    }
    ddl_statement <- ddl %>% {paste(.$ddl,collapse="")}
    ddl_statement <- gsub(paste0(schema_from,'."',table_name,'"'),
                          paste0(schema_to,'."',table_name,'"'),
                          ddl_statement)
    message(paste("creating table with following ddl\n",ddl_statement))
    ddl_res <- strsplit(x=ddl_statement,split=";")[[1]] %>% map(function(x){dbExecute(dbcon_to,x)})
  }

  message('Initiating Redshift table moving for table ',table_name)

  svv <- dbGetQuery(dbcon_from,glue("select * from svv_table_info where schema = '{schema_from}' and \"table\" = '{table_name}'"))

  if(nrow(svv) != 1){
    warning("Cannot locate the source table")
    return(FALSE)
  }

  numRows <- svv$tbl_rows

  if(numRows == 0){
    warning("Empty dataset provided, will not try uploading")
    return(FALSE)
  }

  message(paste0("The provided data.frame has ", numRows, ' rows '))


  # Unload table to S3
  prefix = unloadToS3(table_name,schema_from, bucket, dbcon_from, access_key, secret_key, session_token, region)

  if(wlm_slots>1){
    queryStmt(dbcon,paste0("set wlm_query_slot_count to ", wlm_slots));
  }
  schema_table = paste0(schema_to,".",table_name)
  result = tryCatch({
    stageTable=s3ToRedshift(dbcon_to, schema_table, bucket, prefix, region,
                            access_key, secret_key, session_token, iam_role_arn, additional_params,
                            is_gz=T)

    # Use a single transaction
    queryStmt(dbcon_to, 'begin')

    message("Deleting target table for replacement")
    queryStmt(dbcon_to, sprintf("delete from %s", schema_table))

    message("Insert new rows")
    queryStmt(dbcon_to, sprintf('insert into %s select * from %s', schema_table, stageTable))

    message("Drop staging table")
    queryStmt(dbcon_to, sprintf("drop table %s", stageTable))

    message("Committing changes")
    queryStmt(dbcon_to, "COMMIT;")

    return(TRUE)
  }, error = function(e) {
    warning(e$message)
    queryStmt(dbcon_to, 'ROLLBACK;')
    return(FALSE)
  }, finally = {

    message("Deleting temporary files from S3 bucket")
    deletePrefix(prefix, bucket, split_files, access_key, secret_key, session_token, region)
  })

  return (result)
}
